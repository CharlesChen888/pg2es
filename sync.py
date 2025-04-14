import os
import math
import json
import multiprocessing
import urllib3
import argparse
import datetime
import psycopg2
from psycopg2.extras import LogicalReplicationConnection, ReplicationMessage
from elasticsearch import Elasticsearch, helpers


REPL_SLOT_NAME = "elasticsync"

pg_to_es_types = {
    "character varying": "text",
    "text": "text",
    "integer": "integer",
    "bigint": "long",
    "real": "float",
    "double precision": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp without time zone": "date",
    "timestamp with time zone": "date",
    "uuid": "keyword",
    "numeric": "double",
    "jsonb": "text"
}

pg_to_py_types = {
    "character varying": str,
    "text": str,
    "integer": int,
    "bigint": int,
    "real": float,
    "double precision": float,
    "boolean": bool,
    "date": datetime.datetime,
    "timestamp without time zone": str,
    "timestamp with time zone": str,
    "uuid": str,
    "numeric": float,
    "jsonb": str
}


def clear_all_indices(es_cli: Elasticsearch) -> None:
    indices = es_cli.indices.get(index="*").body
    for index in dict(indices).keys():
        if not str(index).startswith("."):
            es_cli.indices.delete(index=index, ignore_unavailable=True)


def filter_columns(table_name: str, column: str) -> bool:
    include = table_info[table_name]["include"]
    exclude = table_info[table_name]["exclude"]
    primary_key = table_info[table_name]["primary_key"]
    if column == primary_key:
        return True
    if len(include) > 0:
        if column not in include:
            return False
    elif column in exclude:
        return False
    return True


def get_columns(pg_conn: LogicalReplicationConnection, schema_name: str, table_name: str) -> list[tuple[str, str]]:
    query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;"
    cursor = pg_conn.cursor()
    cursor.execute(query, (schema_name, table_name))
    columns = cursor.fetchall()
    cursor.close()
    ret = []
    for column in columns:
        if not filter_columns(table_name, column[0]):
            continue
        if column[1] not in pg_to_es_types.keys():
            continue
        ret.append(column)
    return ret


def get_all_rows(pg_conn: LogicalReplicationConnection, schema_name: str, table_name: str, col_list: list) -> list[tuple]:
    cursor = pg_conn.cursor()
    columns = []
    for col in col_list:
        columns.append(f"\"{col}\"")
    columns_str = ",".join(columns)
    cursor.execute(f"SELECT {columns_str} FROM {schema_name}.\"{table_name}\"")
    rows = cursor.fetchall()
    cursor.close()
    return rows


def gen_index_name(schema_name: str, table_name: str) -> str:
    schema_name = schema_name.lower()
    table_name = table_name.lower()
    return f"{schema_name}.{table_name}"


def create_index(es_cli: Elasticsearch, schema_name: str, table_name: str, columns: list) -> None:
    field_types = {}
    for column in columns:
        field_types[column[0]] = pg_to_es_types[column[1]]
    properties = {}
    for field, field_type in field_types.items():
        properties[field] = {"type": field_type}
        for data_type, filed_info in field_mapping.items():
            if field_type == data_type:
                for key, value in dict(filed_info).items():
                    properties[field][key] = value
                break
    mapping = {"mappings": {"properties": properties}}
    es_cli.indices.create(index=gen_index_name(schema_name, table_name), body=mapping)


def get_json_leaves(json_obj: object, leaves=None) -> list[str]:
    if leaves is None:
        leaves = []
    if isinstance(json_obj, dict):
        for _, value in json_obj.items():
            get_json_leaves(value, leaves)
    elif isinstance(json_obj, list):
        for _, value in enumerate(json_obj):
            get_json_leaves(value, leaves)
    elif isinstance(json_obj, str) or isinstance(json_obj, int) or isinstance(json_obj, float):
        leaves.append(str(json_obj))
    return leaves


def is_nan(number):
    try:
        return math.isnan(number)
    except Exception:
        return False


def gen_documents_from_rows(schema_name: str, table_name: str, columns: list[tuple[str, str]], rows: list[tuple]) -> list[dict]:
    index = gen_index_name(schema_name, table_name)
    documents = []
    for row in rows:
        document = {}
        document["_index"] = index
        for column, value in zip(columns, list(row)):
            if column[1] == "jsonb" and value is not None:
                value = ",".join(get_json_leaves(value))
            elif is_nan(value):
                value = None
            document[column[0]] = value
            if column[0] == table_info[table_name]["primary_key"]:
                document["_id"] = value
        documents.append(document)
    return documents


def gen_actions_from_changes(changes: list) -> list[dict]:
    actions = []
    for change in changes:
        table_name = str(change["table"])
        schema_name = str(change["schema"])
        if f"{schema_name}.{table_name}" not in needed_table:
            continue
        if change["kind"] not in ["delete", "insert", "update"]:
            continue
        index_name = gen_index_name(schema_name, table_name)
        if change["kind"] == "delete":
            action = {
                "_op_type": "delete",
                "_index": index_name,
                "_id": change["oldkeys"]["keyvalues"][0]
            }
        else:
            source = {}
            for columnname, columnvalue in zip(change["columnnames"], change["columnvalues"]):
                if filter_columns(table_name, columnname):
                    data_type = str(table_info[table_name]["columns_type"][columnname])
                    if data_type == "jsonb" and columnvalue is not None:
                        columnvalue = ",".join(get_json_leaves(json.loads(columnvalue)))
                    elif data_type == "date" or data_type.startswith("timestamp"):
                        columnvalue = datetime.datetime.fromisoformat(columnvalue)
                    source[columnname] = columnvalue
            if change["kind"] == "insert":
                action = {
                    "_op_type": "create",
                    "_index": index_name,
                    "_id": source[table_info[table_name]["primary_key"]],
                    "_source": source
                }
            elif change["kind"] == "update":
                action = {
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": source[table_info[table_name]["primary_key"]],
                    "doc": source
                }
        actions.append(action)
    return actions


def es_consumer(msg: ReplicationMessage) -> None:
    try:
        changes = json.loads(msg.payload)["change"]
        actions = gen_actions_from_changes(changes)
        if len(actions) > 0:
            for success, info in helpers.parallel_bulk(es_cli, actions, thread_count=cpu_count, raise_on_error=False):
                if not success:
                    print(info)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
    except Exception as e:
        print(e)


def read_config(file_path: str) -> dict:
    if not os.path.exists(file_path):
        print(f"File not exists: {file_path}")
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"{e}")
        return {}


def handle_config(config: dict) -> tuple[dict, list[str], dict]:
    global_exclude = config["global_exclude"]
    table_info = {}
    table_names = []
    for source in config["sync_source"]:
        schema_name = source["schema"]
        for table in source["tables"]:
            table_name = table["table_name"]
            table_exclude = table["exclude"]
            table_include = table["include"]
            final_exclude = list(set(table_exclude + global_exclude) - set(table_include))
            info = {
                "schema": schema_name,
                "primary_key": table["primary_key"],
                "include": table["include"],
                "exclude": final_exclude
            }
            table_info[table_name] = info
            table_names.append(f"{schema_name}.{table_name}")
    field_mapping = config["field_mapping"]
    return table_info, table_names, field_mapping


if __name__ == "__main__":
    # Get environment.
    cpu_count = multiprocessing.cpu_count()
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "15432")
    pg_dbname = os.getenv("PG_DBNAME", "postgres")
    pg_schema = os.getenv("PG_SCHEMA", "public")
    pg_user = os.getenv("PG_USER", "postgres")
    pg_pswd = os.getenv("PG_PSWD", "1234556")
    es_schema = os.getenv("ES_SCHEMA", "https")
    es_host = os.getenv("ES_HOST", "localhost")
    es_port = os.getenv("ES_PORT", "19200")
    es_user = os.getenv("ES_USER", "elastic")
    es_pswd = os.getenv("ES_PSWD", "1234556")
    # Read JSON config file.
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="Path to config file.")
    parser.add_argument("sync_type", choices=["full", "incr"], help="Full sync or incremental sync.")
    args = parser.parse_args()
    config = read_config(args.config_file)
    table_info, needed_table, field_mapping = handle_config(config)
    sync_type = args.sync_type
    # Connect to PG and ES.
    pg_connection_string = f"host={pg_host} port={pg_port} dbname={pg_dbname} user={pg_user} password={pg_pswd}"
    pg_conn = psycopg2.connect(pg_connection_string, LogicalReplicationConnection)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    es_url = f"{es_schema}://{es_host}:{es_port}"
    es_cli = Elasticsearch(es_url, basic_auth=(es_user, es_pswd), verify_certs=False)
    # Full synchronization.
    if sync_type == "full":
        clear_all_indices(es_cli)
    for table_name, info in table_info.items():
        columns_type = get_columns(pg_conn, info["schema"], table_name)
        table_info[table_name]["columns_type"] = {column[0]: column[1] for column in columns_type}
        if sync_type == "full":
            rows = get_all_rows(pg_conn, info["schema"], table_name, [column[0] for column in columns_type])
            create_index(es_cli, info["schema"], table_name, columns_type)
            docs = gen_documents_from_rows(info["schema"], table_name, columns_type, rows)
            for success, info in helpers.parallel_bulk(es_cli, docs, thread_count=cpu_count, raise_on_error=False):
                if not success:
                    print(info)
    # Incremental synchronization.
    cursor = pg_conn.cursor()
    try:
        cursor.start_replication(slot_name=REPL_SLOT_NAME, decode=True)
    except psycopg2.ProgrammingError:
        cursor.create_replication_slot(REPL_SLOT_NAME, output_plugin="wal2json")
        cursor.start_replication(slot_name=REPL_SLOT_NAME, decode=True)
    try:
        cursor.consume_stream(es_consumer, 60)
    except KeyboardInterrupt:
        cursor.drop_replication_slot(REPL_SLOT_NAME)
        cursor.close()
        pg_conn.close()
        es_cli.close()
    except Exception as e:
        print(e)
