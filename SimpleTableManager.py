import os
import json
import csv
from typing import Dict, List
import operator as oper
from collections import defaultdict


class SimpleDB:
    DATA_TYPES = {"int", "float", "bool", "str", "char"}
    MAX_ROWS_PER_FILE = 64

    operator_mapping = {
            "==": oper.eq,
            "!=": oper.ne,
            ">": oper.gt,
            "<": oper.lt,
            ">=": oper.ge,
            "<=": oper.le,
        }

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
        if not os.path.exists(file_path):
            self._create_csv_file(table_name, file_number)
        with open(file_path, "r+") as file:
            reader = csv.reader(file)
            rows = list(reader)
            row_number = self._get_row_position(row_id)
            if len(rows) <= row_number:
                rows.extend([[] for _ in range(row_number - len(rows) + 1)])
            rows[row_number] = row_data

            file.seek(0)
            file.truncate()  # Truncate the file to remove old content
            writer = csv.writer(file)
            writer.writerows(rows)

    def _create_csv_file(self, table_name, file_number):
        file_path = os.path.join(
            self.datapath, table_name, f"{table_name}_{file_number}.csv"
        )
        with open(file_path, "w") as file:
            pass

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

        # Create the first CSV file
        self._create_csv_file(name, 0)

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

        primary_key_column = self._load_json_file(os.path.join(self.datapath, table_name, f"{table_name}_schema.json"))["primary_key"]
        primary_key_value = values[primary_key_column]
        primary_key_index_path = os.path.join(self.datapath, table_name, f"{table_name}_primary_key_index.json")
        primary_key_index = self._load_json_file(primary_key_index_path)

        # Check for duplicate primary key before allocating new ID
        if primary_key_value in primary_key_index:
            raise ValueError(f"Duplicate primary key value: {primary_key_value}")

        # Allocate row ID and check if it's reused or new
        row_id, is_new_id = self._allocate_row_id(table_name)
        increment_row_id = True and is_new_id

        try:
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
        except Exception:
            if increment_row_id:
                # Decrement the auto_id if the ID was newly allocated
                metadata_path = os.path.join(
                    self.datapath, table_name, f"{table_name}_metadata.json"
                )
                metadata = self._load_json_file(metadata_path)
                metadata["auto_id"] -= 1
                self._save_json_file(metadata_path, metadata)


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
        
        results = []
        for column_name, operator, value in conditions:
            try:
                value = self._check_datatype(value, schema[column_name])
                row_value = row[column_name]
                # Check each condition
                result = self.operator_mapping[operator](row_value, value)
                results.append(result)
            except KeyError:
                results.append(False)
        if logical_operator == "AND":
            return all(results)
        elif logical_operator == "OR":
            return any(results)
        else:
            return results[0] if results else False


    def evaluate_condition(self, item, condition):
        field, operator, value = condition
        field = field.strip('()')
        value = value.strip('()')
        # print(field, operator, value)
        if operator == '>':
            if "=" in value:
                return item[field] >= int(value[1:])
            return item[field] > int(value)
        elif operator == '<':
            if "=" in value:
                return item[field] <= int(value[1:])
            return item[field] < int(value)
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

    
    def get_table_data(self, table_name):
        if not self.check_table_exists(table_name):
            raise FileNotFoundError(f"Table {table_name} does not exist.")

        schema_path = os.path.join(self.datapath, table_name, f"{table_name}_schema.json")
        schema = self._load_json_file(schema_path)

        metadata = self._load_json_file(os.path.join(self.datapath, table_name, f"{table_name}_metadata.json"))
        auto_id = metadata["auto_id"]

        no_of_files = auto_id // self.MAX_ROWS_PER_FILE + 1
        ret = {
            'schema': schema,
            'no_of_files': no_of_files,
        }
        return ret
    
    def merge_chunks_bool(self, chunk_i, chunk_j, join_conditions):
        "join_conditions: list of tuples of the form (table1.column==table2.column)"
        table1_column, table2_column = join_conditions.split('==')
        return chunk_i[table1_column] == chunk_j[table2_column]
    

    def execute_join_query(self, table_name, column_list=None,conditions=None, order_by=None, join_table=None, join_conditions=None):
        result = []
        column_dict = {}
        if column_list:
            for col in column_list:
                if '.' not in col:
                    t_n, column_name = table_name, col
                else:
                    t_n, column_name = col.split('.')
                if t_n not in column_dict:
                    column_dict[t_n] = []
                column_dict[t_n].append(column_name)
        # Process data in chunks
        table_data = self.get_table_data(table_name)
        if table_name not in column_dict or not column_dict[table_name]:
            column_dict[table_name] = list(table_data['schema']['columns'].keys())
        if set(column_dict[table_name]) - set(table_data['schema']['columns'].keys()):
            raise KeyError(f"Columns {column_dict[table_name]} do not exist in table {table_name}.")
        
        if join_table:
            join_table_data = self.get_table_data(join_table)
            if join_table not in column_dict or not column_dict[join_table]:
                column_dict[join_table] = list(join_table_data['schema']['columns'].keys())
            if set(column_dict[join_table]) - set(join_table_data['schema']['columns'].keys()):
                raise KeyError(f"Columns {column_dict[join_table]} do not exist in table {join_table}.")
            
            table_column_names = list(table_data['schema']['columns'].keys())

            for idx,col in enumerate(table_column_names):
                table_column_names[idx] = table_name + '.' + col
            join_table_column_names = list(join_table_data['schema']['columns'].keys())
            for idx,col in enumerate(join_table_column_names):
                join_table_column_names[idx] = join_table + '.' + col

            file_chunks = 0
            
            for file_number_i in range(table_data['no_of_files']):
                for join_file_number_j in range(join_table_data['no_of_files']):
                    chunks_result = []
                    with open(os.path.join(self.datapath, table_name, f"{table_name}_{file_number_i}.csv"), "r") as file_i:  
                        reader_i = csv.reader(file_i)
                        for row_i in reader_i:
                            for idx,col in enumerate(row_i):
                                row_i[idx] = self._check_datatype(col, table_data['schema']['columns'][table_column_names[idx].split('.')[1]])
                            chunk_i = dict(zip(table_column_names, row_i))
                            with open(os.path.join(self.datapath, join_table, f"{join_table}_{join_file_number_j}.csv"), "r") as file_j:
                                reader_j = csv.reader(file_j)
                                for row_j in reader_j:
                                    for idx,col in enumerate(row_j):
                                        row_j[idx] = self._check_datatype(col, join_table_data['schema']['columns'][join_table_column_names[idx].split('.')[1]])
                                    
                                    chunk_j = dict(zip(join_table_column_names, row_j))
                                    if not self.merge_chunks_bool(chunk_i, chunk_j, join_conditions):
                                        continue

                                    chunk_i.update(chunk_j)

                                    filtered_chunk = [chunk_i] if self.evaluate_conditions(chunk_i, conditions) else []
                                    chunks_result.extend(filtered_chunk)

                    # store the result of each file_chunk in a file

                    if order_by:
                        if chunks_result:
                            chunks_result.sort(key=lambda x: x[order_by[0]], reverse=order_by[1] == 'DESC')
                            with open(f"join_{file_chunks}.csv", "w") as file:
                                writer = csv.DictWriter(file, fieldnames=chunks_result[0].keys())

                                writer.writeheader()

                                for row in chunks_result:
                                    writer.writerow(row)

                            file_chunks += 1
                    else:
                        if chunks_result:
                            result.extend(chunks_result)

            if not column_list:
                table_column_names = [table_name + '.' + col for col in table_data['schema']['columns'].keys()]
                join_table_column_names = [join_table + '.' + col for col in join_table_data['schema']['columns'].keys()]
                column_dict[table_name] = table_column_names + join_table_column_names
            else:
                column_dict[table_name] = column_list
        else:
            file_chunks = 0

            for file_number_i in range(table_data['no_of_files']):
                chunks_result = []
                for row_i in self._load_csv_rows_in_a_file(table_name, file_number_i):
                    # Filtering
                    chunk_i = dict(zip(table_data["schema"]["columns"].keys(), row_i))
                    if self.evaluate_conditions(chunk_i, conditions):
                        filtered_chunk = [chunk_i] if self.evaluate_conditions(chunk_i, conditions) else []
                        chunks_result.extend(filtered_chunk)

                # store the result of each file_chunk in a file

                if order_by:
                    if chunks_result:
                        chunks_result.sort(key=lambda x: x[order_by[0]], reverse=order_by[1] == 'DESC')
                        with open(f"join_{file_chunks}.csv", "w") as file:
                            writer = csv.DictWriter(file, fieldnames=chunks_result[0].keys())

                            writer.writeheader()

                            for row in chunks_result:
                                writer.writerow(row)

                        file_chunks += 1
                else:
                    if chunks_result:
                        result.extend(chunks_result)



        
        # merge all the files
        if order_by:
            self.merge([f"join_{i}.csv" for i in range(file_chunks)], f"join_{table_name}_{join_table}.csv", order_by)
            result_file_csv = f"join_{table_name}_{join_table}.csv"
            with open(result_file_csv, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    result.append(row)
            

            os.system(f"rm join_*.csv")
            if os.path.exists(f"rm temp_merged_*.csv"):
                os.system(f"rm temp_merged_*.csv")

        ret = []
        # Selecting fields
        for item in result:
            ret.append([item.get(field, None) for field in column_dict[table_name]])

        
        return column_dict[table_name], ret
    


    
    def perform_group_by(self, filename, group_field, aggregate_func, aggregate_field):
        def process_chunk(chunk, group_results, group_field, aggregate_func, aggregate_field):
            for row in chunk:
                group_value = row[group_field]

                try:
                    aggregate_value = float(row[aggregate_field]) if aggregate_field and row[aggregate_field] else 0
                except ValueError:
                    print(f"Warning: Invalid value for aggregation field '{aggregate_field}' in row: {row}")
                    aggregate_value = 0

                if aggregate_func == "MAX":
                    group_results[group_value]["MAX"] = max(group_results[group_value]["MAX"], aggregate_value)
                elif aggregate_func == "MIN":
                    group_results[group_value]["MIN"] = min(group_results[group_value]["MIN"], aggregate_value)
                elif aggregate_func == "SUM":
                    group_results[group_value]["SUM"] += aggregate_value
                elif aggregate_func == "COUNT":
                    group_results[group_value]["COUNT"] += 1


        file_data = self.get_table_data(filename)
        
        group_results = defaultdict(lambda: {"SUM": 0, "COUNT": 0, "MAX": float('-inf'), "MIN": float('inf')})

        for i in range(file_data['no_of_files']):
            chunk = []
            for row in self._load_csv_rows_in_a_file(filename, i):
                row_data = dict(zip(file_data["schema"]["columns"].keys(), row))
                chunk.append(row_data)

            if chunk:
                process_chunk(chunk, group_results, group_field, aggregate_func, aggregate_field)
        # Print or process the group results as needed
        ret_col = [group_field, aggregate_func]
        ret = []
        for group, values in group_results.items():
            ret.append([group, values[aggregate_func]])
        return ret_col, ret


    def merge(self,file_list, output_filepath, order_by):

        def merge_two(file1, file2, output_file, col, order):
            order = False if order == 'DESC' else True
            with open(file1, 'r', newline='', encoding='utf-8') as f1, \
                    open(file2, 'r', newline='', encoding='utf-8') as f2, \
                    open(output_file, 'w', newline='', encoding='utf-8') as f_out:

                reader1, reader2 = csv.DictReader(f1), csv.DictReader(f2)
                writer = csv.writer(f_out)

                writer.writerow(reader1.fieldnames)

                # Initialize rows
                row1, row2 = next(reader1, None), next(reader2, None)

                # print(row1, row2)
                while row1 is not None or row2 is not None:
                    # print(row1, row2)
                    if order:
                        condition = row2 is None or (row1 is not None and row1[col] < row2[col])
                    else:
                        condition = row2 is None or (row1 is not None and row1[col] > row2[col])

                    if condition:
                        # print("1 ", row1)
                        writer.writerow(row1.values())
                        row1 = next(reader1, None)
                    else:
                        # print("2 ", row2)
                        writer.writerow(row2.values())
                        row2 = next(reader2, None)


        j = 0

        while len(file_list) > 1:
            new_file_list = []
            for i in range(0, len(file_list), 2):
                if i + 1 < len(file_list):
                    output_file = f'temp_merged_{j}.csv'
                    merge_two(file_list[i], file_list[i + 1], output_file, col=order_by[0], order=order_by[1])
                    new_file_list.append(output_file)
                    j += 1
                else:
                    new_file_list.append(file_list[i])

            file_list = new_file_list

        # Rename the last remaining file to the desired output files
        os.rename(file_list[0], output_filepath)