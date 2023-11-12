"""
cli_parser.py
This module parses SQL queries and returns a dictionary with the parsed query
"""
import re
import json
import new_database as db

DATA_TYPES = {"int", "float", "bool", "str", "char"}


def parse_create_table(query):
    """
    parse_create_table parses a create table query
    :param query: Create table query to parse
    """
    create_table_pattern = r"make\s+table\s+named\s+(\w+)\s+with\s+columns\s+(.*)"
    match = re.match(create_table_pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid create table query")

    table_name, columns = match.group(1), match.group(2)
    column_list = [col.strip().split(" typed ") for col in columns.split(",")]
    column_list = {col[0]: col[1] for col in column_list}

    return {"command": "create_table", "table_name": table_name, "columns": column_list}


def parse_insert(query):
    """
    Parses an insert query
    :param query: Insert query to parse
    """
    insert_pattern = r"add\s+an\s+entry\s+to\s+(\w+)\s+with\s+(.*)"
    match = re.match(insert_pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid insert query")

    table_name, values = match.group(1), match.group(2).split(",")
    value_list = [val.strip().split("=") for val in values]

    return {
        "command": "insert",
        "table_name": table_name,
        "values": {val[0]: val[1] for val in value_list},
    }


def parse_delete(query):
    """
    Parses a delete query
    :param query: Delete query to parse
    """
    delete_pattern = r"delete\s+entry\s+with\s+(.*)\s+from\s+(\w+)"
    match = re.match(delete_pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid delete query")

    condition, table_name = match.group(1), match.group(2)

    return {"command": "delete", "table_name": table_name, "condition": condition}


def parse_update(query: str):
    """
    Parses an update query
    :param query: Update query to parse
    """

    update_pattern = (
        r"modify\s+entry\s+with\s+(\w+)\s+(\w+)\s+from\s+(\w+)\s+with\s+(.*)"
    )
    match = re.match(update_pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid update query")

    id_column_name, id_value, table_name, values = (
        match.group(1),
        match.group(2),
        match.group(3),
        match.group(4).split(","),
    )
    value_list = [val.strip().split("=") for val in values]

    return {
        "command": "update",
        "table_name": table_name,
        "id_column_name": id_column_name,
        "id_value": id_value,
        "values": {val[0]: val[1] for val in value_list},
    }


def parse_query(query):
    """
    Parses a query
    :param query: Query to parse
    """
    query_pattern = r"get\s+all\s+entries\s+from\s+(\w+)\s+where\s+(.*)"
    match = re.match(query_pattern, query, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid query")

    table_name, conditions = match.group(1), match.group(2)
    condition_list = [cond.strip() for cond in conditions.split(",")]

    return {"command": "query", "table_name": table_name, "conditions": condition_list}


def parse_sql(sql_query: str):
    """
    Parses a SQL query
    :param sql_query: SQL query to parse
    """

    query = sql_query
    if query.lower().startswith("make table"):
        ret = parse_create_table(query)
        for data_type in ret["columns"].values():
            if data_type not in DATA_TYPES:
                return {"error": f"Invalid data type: {data_type}"}
        if not db.check_table_exists(ret["table_name"]):
            dbb = db.FlatFileDB(ret["table_name"])
            dbb.create_table(**ret["columns"])
            return ret
        return {"error": "Table already exists"}
    elif query.lower().startswith("add an entry"):
        ret = parse_insert(query)
        print(ret)
        if db.check_table_exists(ret["table_name"]):
            dbb = db.FlatFileDB(ret["table_name"])
            dbb.insert(**ret["values"])
            return ret
        return {"error": "Table does not exist"}
    elif query.lower().startswith("delete entry"):
        return parse_delete(query)
    elif query.lower().startswith("modify entry"):
        ret = parse_update(query)
        if db.check_table_exists(ret["table_name"]):
            dbb = db.FlatFileDB(ret["table_name"])
            dbb.update(ret["id_column_name"], ret["id_value"], **ret["values"])
            return ret
        return ret
    elif query.lower().startswith("get all entries"):
        return parse_query(query)
    else:
        # parsed_query = parse_select(query)
        # parsed_query['command'] = 'select'
        # return parsed_query
        raise ValueError("Invalid query")


if __name__ == "__main__":
    # Example SQL queries
    create_table_query = "make table named table_name with columns col1 typed type1, col2 typed type2, col3 typed type3"
    insert_query = (
        "add an entry to table_name with col1=value1, col2=value2, col3=value3"
    )
    delete_query = "delete entry with col1=value1 from table_name"
    update_query = "modify entry with id id from table_name with col1=value1, col2=value2, col3=value3"
    query_query = "get all entries from table_name where col1=value1, col2=value2"

    parsed_create_table = parse_sql(create_table_query)
    print(json.dumps(parsed_create_table, indent=4))
    parsed_insert = parse_sql(insert_query)
    print(json.dumps(parsed_insert, indent=4))
    parsed_delete = parse_sql(delete_query)
    print(json.dumps(parsed_delete, indent=4))
    parsed_update = parse_sql(update_query)
    print(json.dumps(parsed_update, indent=4))
    parsed_query = parse_sql(query_query)
    print(json.dumps(parsed_query, indent=4))

    # print(json.dumps(parsed_create_table, indent=4))


# modify entry with index 54 from student with age=23
