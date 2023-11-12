"""This module implements a flat file database with the following features:"""
import csv
import os
import json
from collections import OrderedDict, defaultdict

DATAPATH = os.path.join(os.getcwd(), "data")
if not os.path.exists(DATAPATH):
    os.mkdir(DATAPATH)


def check_table_exists(name):
    """
    Function to check if a table (file) exists in the current working directory
    :param name: name of the table to check
    """
    return os.path.exists(DATAPATH + "/" + name)


def check_metadata_exists(name):
    """
    Function to check if a metadata file exists in the current working directory
    :param name: name of the table to check
    """
    return os.path.exists(DATAPATH + "/" + f"{name}/{name}_metadata.json")


class FlatFileDB:
    """
    A simple flat-file database implementation.

    This class provides basic functionality for creating, inserting, updating, and querying data in a flat-file database.
    The database is stored as a collection of CSV files, with one file per 64 records.
    The schema for the database is stored in a separate JSON file.
    """

    def __init__(self, name):
        """Initialize the database with the given name."""
        self.name = name
        if not check_table_exists(self.name):
            os.mkdir(DATAPATH + "/" + self.name)
        self.index = {}
        self.column_indices = defaultdict(dict)
        if os.path.exists(DATAPATH + "/" + f"{self.name}/{self.name}_schema.json"):
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_schema.json",
                "r",
                encoding="utf-8",
            ) as f:
                self.schema = json.load(f)
        else:
            self.schema = {}
        self.load_metadata()
        self.load_indices()

    def create_table(self, **columns):
        """
        Create a new table with the given columns.
        :param columns: a dictionary of column names and their data types

        """
        self.schema = columns
        with open(
            DATAPATH + "/" + f"{self.name}/{self.name}_schema.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(self.schema, f)
        self.load_metadata()
        self.load_indices()

    def insert(self, **values):
        """
        Insert a new record into the table.
        :param values: a dictionary of column names and their values
        """
        type_conversions = {"char": str, "str": str, "int": int, "float": float, "bool": bool}

        # Check and convert column types
        for col, dtype in self.schema.items():
            if col not in values:
                raise ValueError(f"Missing value for column: {col}")
            dtype = "str" if dtype == "char" else dtype
            if dtype not in type_conversions:
                raise ValueError(f"Unsupported column type: {dtype}")
            try:
                values[col] = type_conversions[dtype](values[col])
            except Exception as exc:
                raise ValueError(f"Invalid value for column: {col}") from exc

        if self.deleted_ids:
            id = self.deleted_ids.pop()
            str_id = str(id)
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_{id >> 6}.csv", "r+", encoding="utf-8"
            ) as f:
                lines = f.readlines()
                f.seek(0)
                for line in lines:
                    if line.startswith(f"{str_id},"):
                        row = line.strip().split(",")
                        for col, val in values.items():
                            index = list(self.schema.keys()).index(col) + 1
                            row[index] = str(val)
                        line = ",".join(row) + "\n"
                    f.write(line)
                f.truncate()
        else:
            self.auto_id += 1
            str_id = str(self.auto_id)
            if not os.path.exists(
                DATAPATH + "/" + f"{self.name}/{self.name}_{self.auto_id >> 6}.csv"
            ):
                with open(
                    DATAPATH + "/" + f"{self.name}/{self.name}_{self.auto_id >> 6}.csv",
                    "w",
                    newline="",
                    encoding="utf-8",
                ) as f:
                    pass
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_{self.auto_id >> 6}.csv",
                "a",
                newline="",
                encoding="utf-8",
            ) as f:
                writer = csv.writer(f)
                ret_row = []
                for col in self.schema.keys():
                    if col in values:
                        ret_row.append(str(values[col]))
                    else:
                        ret_row.append("")
                row = [str_id] + ret_row
                writer.writerow(row)

        self.update_indices(str_id, values)

        self.save_metadata()

    def delete(self, id):
        str_id = str(id)
        if not self.row_exists(id):
            raise Exception("Record not found")

        row = self.get_row_by_id(id)
        for idx, col in enumerate(self.schema.keys(), 1):
            value = self.convert_col_value_to_given_data_type(col, row[idx])
            if (
                value in self.column_indices[col]
                and str_id in self.column_indices[col][value]
            ):
                del self.column_indices[col][value][str_id]
                if not self.column_indices[col][value]:
                    del self.column_indices[col][value]

        self.mark_as_deleted(str_id)
        self.save_metadata()
        self.save_indices()


    def get_id_from_index(self, col, value):
        """
        Get the id for the given value in the given column.
        
        :param col: The column name where the value is to be searched.
        :param value: The value for which the ID needs to be found.
        :return: A dictionary with either the found ID or an error message.
        """
        # Check if the column is valid
        if col not in self.schema.keys():
            return {"error": f"Invalid column index name: {col}"}
        
        # Construct the file path
        file_path = f"{DATAPATH}/{self.name}/{self.name}_{col}_index.json"
        
        try:
            # Open the index file and load the data
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Check if the value exists in the data and return the corresponding ID
            if value in data:
                return {"id": list(data[value].keys())[0]}
            else:
                return {"error": f"No record exists with {col} = {value}"}
        
        except FileNotFoundError:
            return {"error": f"Index file for column {col} does not exist at {file_path}"}
        except json.JSONDecodeError:
            return {"error": f"Index file for column {col} is not a valid JSON file"}
        
        

    def update(self, col_name, col_val, **values):
        """
        Update the record with the given ID with the given values.
        :param col_name: The column name where the value is to be searched.
        :param col_val: The value for which the ID needs to be found.
        :param values: a dictionary of column names and their values
        """
        record_id = self.get_id_from_index(col_name, col_val)
        if "error" in record_id:
            return record_id["error"]
        else:
            record_id = int(record_id["id"])
        str_id = str(record_id)
        if not self.row_exists(record_id):
            raise ValueError("Record not found")

        original_row = self.get_row_by_id(record_id)
        for col, new_val in values.items():
            if col not in self.schema:
                raise ValueError(f"Invalid column: {col}")
            col_type = self.schema[col]
            if col_type == "int":
                new_val = int(new_val)
            elif col_type == "float":
                new_val = float(new_val)
            elif col_type == "bool":
                new_val = bool(new_val)
            elif col_type == "str":
                new_val = str(new_val)
            elif col_type == "char":
                new_val = str(new_val)
            else:
                raise ValueError(f"Unsupported column type: {col_type}")

            old_val = original_row[list(self.schema.keys()).index(col) + 1]
            old_val = self.convert_col_value_to_given_data_type(col, old_val)
            self.update_indices(str_id, {col: new_val}, old_val=old_val)

        with open(DATAPATH + "/" + f"{self.name}/{self.name}_{record_id>>6}.csv", "r+", encoding="utf-8") as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if line.startswith(f"{str_id},"):
                    row = line.strip().split(",")
                    for col, val in values.items():
                        index = list(self.schema.keys()).index(col) + 1
                        row[index] = str(val)
                    line = ",".join(row) + "\n"
                f.write(line)
            f.truncate()

        self.save_metadata()

    def query(self, **conditions):
        results = []
        candidate_ids = set()
        for col, value in conditions.items():
            if col not in self.schema:
                raise Exception("Invalid column in conditions")
            ids_for_value = set(
                map(int, self.column_indices[col].get(value, {}).keys())
            )
            candidate_ids.update(ids_for_value)

        ids_by_file = defaultdict(set)
        while candidate_ids:
            id = candidate_ids.pop()
            ids_by_file[id >> 6].add(id)
        del candidate_ids

        for file_id in ids_by_file.keys():
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_{file_id}.csv", "r", encoding="utf-8"
            ) as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row[1] == "" and int(row[0]) in ids_by_file[file_id]:
                        results.append(
                            OrderedDict(zip(list(self.schema.keys()), row[1:]))
                        )
                f.close()
        return results

    def load(self):
        if os.path.exists(DATAPATH + "/" + f"{self.name}/{self.name}_schema.json"):
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_schema.json",
                "r",
                encoding="utf-8",
            ) as f:
                self.schema = json.load(f)

        for col in self.schema.keys():
            index_file = DATAPATH + "/" + f"{self.name}/{self.name}_{col}_index.json"
            if os.path.exists(index_file):
                with open(index_file, "r", encoding="utf-8") as f:
                    self.column_indices[
                        col
                    ] = self.convert_index_keys_to_given_data_type(col, json.load(f))

    def convert_index_keys_to_given_data_type(self, col, dictt):
        """
        Convert the keys of the index dictionary to the given data type.
        """
        if self.schema[col] == "int":
            return {int(k): v for k, v in dictt.items()}
        elif self.schema[col] == "float":
            return {float(k): v for k, v in dictt.items()}
        elif self.schema[col] == "bool":
            return {bool(k): v for k, v in dictt.items()}
        else:
            return dictt

    def convert_col_value_to_given_data_type(self, col, value):
        """
        Convert the given value to the given data type.
        """
        if self.schema[col] == "int":
            return int(value)
        elif self.schema[col] == "float":
            return float(value)
        elif self.schema[col] == "bool":
            return bool(value)
        else:
            return value

    def row_exists(self, record_id):
        return record_id <= self.auto_id and record_id > -1

    def get_row_by_id(self, id):
        with open(DATAPATH + "/" + f"{self.name}/{self.name}_{id>>6}.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if int(row[0]) == id and not row[0] == "":
                    return row

    def save_metadata(self):
        """
        Save the metadata for the database, including the auto_id and the list of deleted ids.
        """
        metadata_file = DATAPATH + "/" + f"{self.name}/{self.name}_metadata.json"
        metadata = {"auto_id": self.auto_id, "deleted_ids": self.deleted_ids}
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f)

    def load_metadata(self):
        """
        Load the metadata for the database, including the auto_id and the list of deleted ids.
        """
        metadata_file = DATAPATH + "/" + f"{self.name}/{self.name}_metadata.json"
        if check_metadata_exists(self.name):
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                self.auto_id = metadata.get("auto_id", -1)
                self.deleted_ids = metadata.get("deleted_ids", [])
        else:
            self.auto_id = -1
            self.deleted_ids = []
            data = {"auto_id": self.auto_id, "deleted_ids": self.deleted_ids}
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(data, f)

    def update_indices(self, str_id, values, old_val=None):
        """
        Update the indices for the given id with the given values.
        """
        for col, new_val in values.items():
            if new_val in self.column_indices[col]:
                self.column_indices[col][new_val][str_id] = ""
            else:
                self.column_indices[col][new_val] = {str_id: ""}

            if old_val is not None:
                if (
                    old_val in self.column_indices[col]
                    and str_id in self.column_indices[col][old_val]
                ):
                    del self.column_indices[col][old_val][str_id]
                    if not self.column_indices[col][old_val]:
                        del self.column_indices[col][old_val]
        self.save_indices()

    def mark_as_deleted(self, str_id):
        """
        Mark the given id as deleted.
        """
        with open(
            DATAPATH + "/" + f"{self.name}/{self.name}_{int(str_id)>>6}.csv",
            "r+",
            encoding="utf-8",
        ) as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if line.startswith(f"{str_id},"):
                    line = (
                        f"{str_id},"
                        + ",".join(["" for _ in range(len(self.schema))])
                        + "\n"
                    )
                f.write(line)
            f.truncate()
        self.deleted_ids.append(int(str_id))

    def save_indices(self):
        """Save the indices for the database."""
        for col, index_data in self.column_indices.items():
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_{col}_index.json",
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(index_data, f)

    def load_indices(self):
        """Load the indices for the database."""
        for col in self.schema.keys():
            index_file = DATAPATH + "/" + f"{self.name}/{self.name}_{col}_index.json"
            if os.path.exists(index_file):
                with open(index_file, "r", encoding="utf-8") as f:
                    self.column_indices[
                        col
                    ] = self.convert_index_keys_to_given_data_type(col, json.load(f))
            else:
                with open(index_file, "w", encoding="utf-8") as f:
                    json.dump({}, f)

    def print_table(self):
        """Print the table to the console."""
        for i in range(self.auto_id + 1):
            with open(
                DATAPATH + "/" + f"{self.name}/{self.name}_{i>>6}.csv",
                "r",
                encoding="utf-8",
            ) as f:
                reader = csv.reader(f)
                for row in reader:
                    if row[1] != "":
                        print(row)


if __name__ == "__main__":
    db = FlatFileDB("my_table")
    db.load()
    db.create_table(name="str", age="int")

    db.insert(name="Alice", age=30)
    db.insert(name="Bob", age=40)
    db.insert(name="Charlie", age=50)

    print("Before Delete:")
    db.print_table()

    db.delete(1)

    print("After Delete:")
    db.print_table()

    db.update("index",0,name="Alicia", age=35,)

    print("After Update:")
    db.print_table()

    print("Query Result:")
    print(db.query(age=50))

    db.insert(name="Max", age=23)

    db.insert(name="Carlos", age=35)
