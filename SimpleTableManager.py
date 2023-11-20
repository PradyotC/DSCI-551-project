import os
import json
import csv
from typing import Dict, List
import operator as oper


class SimpleDB:
    DATA_TYPES = {"int", "float", "bool", "str", "char"}
    MAX_ROWS_PER_FILE = 64

    def __init__(self):
        self.datapath = os.path.join(os.getcwd(), "data")
        if not os.path.exists(self.datapath):
            os.makedirs(self.datapath)

    def _check_datatype(self, value, datatype):
        try:
            if datatype == "int":
                return int(value)
            elif datatype == "float":
                return float(value)
            elif datatype == "bool":
                if value.lower() in ["true", "false"]:
                    return value.lower() == "true"
                raise ValueError
            elif datatype in {"str", "char"}:
                return value
            else:
                raise ValueError("Invalid datatype")
        except ValueError:
            raise ValueError(f"Value {value} cannot be converted to {datatype}")

    def _get_file_number(self, row_id):
        return row_id >> 6

    def _get_row_position(self, row_id):
        return row_id % self.MAX_ROWS_PER_FILE

    def _load_json_file(self, file_path):
        with open(file_path, "r") as file:
            return json.load(file)

    def _save_json_file(self, file_path, data):
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

    def _update_csv_file(self, table_name, row_id, row_data):
        file_number = self._get_file_number(row_id)
        file_path = os.path.join(
            self.datapath, table_name, f"{table_name}_{file_number}.csv"
        )

        with open(file_path, "r+") as file:
            reader = csv.reader(file)
            rows = list(reader)
            rows[self._get_row_position(row_id)] = row_data

            file.seek(0)
            file.truncate()  # Truncate the file to remove old content
            writer = csv.writer(file)
            writer.writerows(rows)

    def _load_csv_rows(self, table_name, start_row=0, end_row=None):
        start_file = self._get_file_number(start_row)
        end_file = self._get_file_number(end_row) if end_row is not None else start_file
        data = []

        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)

        for file_number in range(start_file, end_file + 1):
            file_path = os.path.join(
                self.datapath, table_name, f"{table_name}_{file_number}.csv"
            )
            with open(file_path, "r") as file:
                reader = csv.reader(file)
                rows = list(reader)
                if file_number == start_file:
                    rows = rows[self._get_row_position(start_row) :]
                if file_number == end_file and end_row is not None:
                    rows = rows[: self._get_row_position(end_row) + 1]

                for row in rows:
                    converted_row = [
                        self._check_datatype(value, datatype)
                        for value, datatype in zip(row, schema["columns"].values())
                    ]
                    data.append(converted_row)

        return data

    def _load_csv_rows_in_a_file(self, table_name, file_number=0):
        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)
        data = []

        file_path = os.path.join(
            self.datapath, table_name, f"{table_name}_{file_number}.csv"
        )
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            rows = list(reader)
            for row in rows:
                "if row doesn't contain any data or contain blank columns then skip it"
                if not any(row):
                    continue
                converted_row = [
                    self._check_datatype(value, datatype)
                    for value, datatype in zip(row, schema["columns"].values())
                ]
                data.append(converted_row)

        return data

    def check_table_exists(self, name):
        return os.path.exists(os.path.join(self.datapath, name))

    def create_table(self, name, columns: Dict[str, str], primary_key):
        if self.check_table_exists(name):
            raise FileExistsError(f"Table {name} already exists.")

        table_path = os.path.join(self.datapath, name)
        os.makedirs(table_path)

        schema = {"columns": columns, "primary_key": primary_key}
        metadata = {"auto_id": -1, "deleted_ids": []}
        primary_key_index = {}

        self._save_json_file(os.path.join(table_path, f"{name}_schema.json"), schema)
        self._save_json_file(
            os.path.join(table_path, f"{name}_metadata.json"), metadata
        )
        self._save_json_file(
            os.path.join(table_path, f"{name}_primary_key_index.json"),
            primary_key_index,
        )

    def _allocate_row_id(self, table_name):
        metadata_path = os.path.join(
            self.datapath, table_name, f"{table_name}_metadata.json"
        )
        metadata = self._load_json_file(metadata_path)

        if metadata["deleted_ids"]:
            return metadata["deleted_ids"].pop(), False  # False indicates a reused ID
        else:
            metadata["auto_id"] += 1
            self._save_json_file(metadata_path, metadata)
            return metadata["auto_id"], True  # True indicates a new ID

    def _prepare_row_data(self, table_name, values: Dict[str, str]):
        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)

        row_data = []
        for column, data_type in schema["columns"].items():
            if column not in values:
                raise KeyError(f"Column {column} missing from values.")
            converted_value = self._check_datatype(values[column], data_type)
            row_data.append(str(converted_value))

        return row_data

    def insert(self, table_name, values: Dict[str, str]):
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        # Prepare row data first
        row_data = self._prepare_row_data(table_name, values)

        primary_key_column = self._load_json_file(
            os.path.join(self.datapath, table_name, f"{table_name}_schema.json")
        )["primary_key"]
        primary_key_value = values[primary_key_column]
        primary_key_index_path = os.path.join(
            self.datapath, table_name, f"{table_name}_primary_key_index.json"
        )
        primary_key_index = self._load_json_file(primary_key_index_path)

        # Check for duplicate primary key before allocating new ID
        if primary_key_value in primary_key_index:
            raise ValueError(f"Duplicate primary key value: {primary_key_value}")

        # Allocate row ID and check if it's reused or new
        row_id, is_new_id = self._allocate_row_id(table_name)

        # Write data to the CSV file
        self._update_csv_file(table_name, row_id, row_data)

        if not is_new_id:
            # Update the metadata file if an ID from deleted_ids was used
            metadata_path = os.path.join(
                self.datapath, table_name, f"{table_name}_metadata.json"
            )
            metadata = self._load_json_file(metadata_path)
            # Ensure the used ID is removed from deleted_ids
            metadata["deleted_ids"] = [
                id for id in metadata["deleted_ids"] if id != row_id
            ]
            self._save_json_file(metadata_path, metadata)

        # Update the primary key index
        primary_key_index[primary_key_value] = row_id
        self._save_json_file(primary_key_index_path, primary_key_index)

    def update(self, table_name, column_name, column_value, values: Dict[str, str]):
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)

        if column_name not in schema["columns"]:
            raise KeyError(
                f"Column {column_name} does not exist in table {table_name}."
            )

        primary_key_index_path = os.path.join(
            self.datapath, table_name, f"{table_name}_primary_key_index.json"
        )
        primary_key_index = self._load_json_file(primary_key_index_path)

        if column_value not in primary_key_index:
            raise KeyError(f"Primary key value {column_value} does not exist.")

        if column_name == schema["primary_key"]:
            new_primary_key_value = values[column_name]
            if new_primary_key_value != column_value:
                if new_primary_key_value in primary_key_index:
                    raise ValueError(
                        f"Duplicate primary key value: {new_primary_key_value}"
                    )

                row_id = primary_key_index.pop(column_value)
                primary_key_index[new_primary_key_value] = row_id
                self._save_json_file(primary_key_index_path, primary_key_index)
            else:
                row_id = primary_key_index[column_value]
        else:
            row_id = primary_key_index[column_value]

        row_data = self._prepare_row_data(table_name, values)
        self._update_csv_file(table_name, row_id, row_data)

    def delete(self, table_name, column_name, column_value):
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)

        if column_name != schema["primary_key"]:
            raise ValueError(
                f"Delete can only be performed on primary key. {column_name} is not a primary key."
            )

        primary_key_index_path = os.path.join(
            self.datapath, table_name, f"{table_name}_primary_key_index.json"
        )
        primary_key_index = self._load_json_file(primary_key_index_path)

        if column_value not in primary_key_index:
            raise KeyError(f"Primary key value {column_value} does not exist.")

        row_id = primary_key_index.pop(column_value)
        self._save_json_file(primary_key_index_path, primary_key_index)

        # Update the CSV file by clearing the row
        self._update_csv_file(table_name, row_id, ["" for _ in schema["columns"]])

        # Update metadata only after successfully clearing the row
        metadata_path = os.path.join(
            self.datapath, table_name, f"{table_name}_metadata.json"
        )
        metadata = self._load_json_file(metadata_path)
        metadata["deleted_ids"].append(row_id)
        self._save_json_file(metadata_path, metadata)

    def check_condition(self, schema, row, conditions, logical_operator):
        operator_mapping = {
            "==": oper.eq,
            "!=": oper.ne,
            ">": oper.gt,
            "<": oper.lt,
            ">=": oper.ge,
            "<=": oper.le,
        }
        results = []
        for column_name, operator, value in conditions:
            try:
                value = self._check_datatype(value, schema[column_name])
                row_value = row[column_name]
                # Check each condition
                result = operator_mapping[operator](row_value, value)
                results.append(result)
            except KeyError:
                results.append(False)
        if logical_operator == "AND":
            return all(results)
        elif logical_operator == "OR":
            return any(results)
        else:
            return results[0] if results else False

    def select(
        self,
        table_name,
        column_list: List[str] = None,
        conditions=None,
        logical_operator=None,
    ):
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        schema_path = os.path.join(
            self.datapath, table_name, f"{table_name}_schema.json"
        )
        schema = self._load_json_file(schema_path)

        if column_list is None:
            column_list = list(schema["columns"].keys())

        missing_columns = set(column_list) - set(schema["columns"].keys())
        if missing_columns:
            raise KeyError(
                f"Columns {missing_columns} do not exist in table {table_name}."
            )

        colindexmap = {col: i for i, col in enumerate(schema["columns"].keys())}
        colindex = sorted([colindexmap[col] for col in column_list])

        result = []
        metadata = self._load_json_file(
            os.path.join(self.datapath, table_name, f"{table_name}_metadata.json")
        )
        auto_id = metadata["auto_id"]

        start_file = self._get_file_number(0)
        end_file = self._get_file_number(auto_id)

        file_number = start_file

        for file_number in range(start_file, end_file + 1):
            for row in self._load_csv_rows_in_a_file(table_name, file_number):
                row_data = dict(zip(schema["columns"].keys(), row))

                if not conditions or self.check_condition(
                    schema["columns"], row_data, conditions, logical_operator
                ):
                    ret_row = []
                    for i in colindex:
                        ret_row.append(row[i])
                    result.append(ret_row)

        return column_list, result


    def evaluate_condition(self, item, condition):
        field, operator, value = condition
        field = field.strip('()')
        value = value.strip('()')
        # print(field, operator, value)
        if operator == '>':
            return item[field] > int(value)
        elif operator == '<':
            return item[field] < int(value)
        elif operator == '>=':
            return item[field] >= int(value)
        elif operator == '<=':
            return item[field] <= int(value)
        elif operator == '==':
            return str(item[field]) == value
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def evaluate_conditions(self, item, conditions):
        if not conditions:
            return True

        def eval_logical_ops(operand1, operator, operand2):
            # print(operand1, operator, operand2)
            if operator == 'AND':
                return operand1 and operand2
            elif operator == 'OR':
                return operand1 or operand2

        def recursive_eval(conds):
            # print(conds)
            if not conds:
                return True
            
            if 'AND' in conds or 'OR' in conds:
                for idx, val in enumerate(conds):
                    if val in ['AND', 'OR']:
                        left, right = conds[:idx], conds[idx+1:]
                        if type(left[0]) == list:
                            left_result = recursive_eval(left[0])
                        else:
                            left_result = recursive_eval(left)
                        if type(right[0]) == list:
                            right_result = recursive_eval(right[0])
                        else:
                            right_result = recursive_eval(right)
                        return eval_logical_ops(left_result, val, right_result)

            return self.evaluate_condition(item, conds)


        return recursive_eval(conditions)

    def execute_query(self, table_name, column_list=None, conditions=None, order_by=None):
        # print(fields, conditions, order_by)
        result = []

        # Process data in chunks
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        schema_path = os.path.join(self.datapath, table_name, f"{table_name}_schema.json")
        schema = self._load_json_file(schema_path)

        if column_list is None:
            column_list = list(schema["columns"].keys())

        missing_columns = set(column_list) - set(schema["columns"].keys())
        if missing_columns:
            raise KeyError(f"Columns {missing_columns} do not exist in table {table_name}.")


        metadata = self._load_json_file(os.path.join(self.datapath, table_name, f"{table_name}_metadata.json"))
        auto_id = metadata["auto_id"]

        start_file = self._get_file_number(0)
        end_file = self._get_file_number(auto_id)

        file_number = start_file

        for file_number in range(start_file, end_file + 1):
            for row in self._load_csv_rows_in_a_file(table_name, file_number):
                # Filtering
                chunk = dict(zip(schema["columns"].keys(), row))
                filtered_chunk = [chunk] if self.evaluate_conditions(chunk, conditions) else []

                # Ordering
                if order_by:
                    filtered_chunk.sort(key=lambda x: x[order_by])

                # Selecting fields
                for item in filtered_chunk:
                    # result_item = {field: item.get(field, None) for field in column_list}
                    # result.append(result_item)
                    result.append([item.get(field, None) for field in column_list])

        return column_list, result


# Example of usage
if __name__ == "__main__":
    db = SimpleDB()
    db.create_table(
        "employees", {"name": "str", "age": "int", "department": "str"}, "name"
    )
    db.insert("employees", {"name": "Alice", "age": 30, "department": "HR"})
    db.update(
        "employees", "name", "Alice", {"name": "Alice", "age": 31, "department": "HR"}
    )
    employees = db.select("employees", ["name", "age"])
    print(employees)
