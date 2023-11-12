import csv
import os
import json
from collections import OrderedDict, defaultdict

class FlatFileDB:
    def __init__(self, name):
        self.name = name
        self.index = {}
        self.column_indices = defaultdict(dict)
        self.auto_id = 0
        self.schema = {}

    def create_table(self, **columns):
        self.schema = columns
        with open(f"{self.name}_schema.json", 'w') as f:
            json.dump(self.schema, f)

    def insert(self, **values):
        for col, dtype in self.schema.items():
            if col not in values:
                raise Exception(f"Missing value for column: {col}")
            if not isinstance(values[col], eval(dtype)):
                raise Exception(f"Incorrect type for column: {col}")
        
        self.auto_id += 1
        str_id = str(self.auto_id)
        
        for col, value in values.items():
            if value in self.column_indices[col]:
                self.column_indices[col][value][str_id] = ""
            else:
                self.column_indices[col][value] = {str_id: ""}

        with open(f"{self.name}.csv", 'a', newline='') as f:
            writer = csv.writer(f)
            row = [self.auto_id] + list(values.values())
            writer.writerow(row)

        self.save_indices()

    def delete(self, id):
        str_id = str(id)
        if not self.row_exists(id):
            raise Exception("Record not found")
        
        row = self.get_row_by_id(id)
        for idx, col in enumerate(self.schema.keys(), 1):
            value = self.convert_col_value_to_given_data_type(col, row[idx])
            if value in self.column_indices[col] and str_id in self.column_indices[col][value]:
                del self.column_indices[col][value][str_id]
                if not self.column_indices[col][value]:
                    del self.column_indices[col][value]
        
        self.save_indices()
        
        with open(f"{self.name}.csv", 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if line.startswith(f"{str_id},"):
                    continue
                f.write(line)
            f.truncate()


    def update(self, id, **values):
        str_id = str(id)
        if not self.row_exists(id):
            raise Exception("Record not found")

        original_row = self.get_row_by_id(id)
        for col, new_val in values.items():
            if col not in self.schema:
                raise Exception(f"Invalid column: {col}")
            if not isinstance(new_val, eval(self.schema[col])):
                raise Exception(f"Incorrect type for column: {col}")
            
            old_val = original_row[list(self.schema.keys()).index(col) + 1]
            old_val = self.convert_col_value_to_given_data_type(col, old_val)
            if old_val in self.column_indices[col] and str_id in self.column_indices[col][old_val]:
                del self.column_indices[col][old_val][str_id]
                if not self.column_indices[col][old_val]:
                    del self.column_indices[col][old_val]
            
            if new_val in self.column_indices[col]:
                self.column_indices[col][new_val][str_id] = ""
            else:
                self.column_indices[col][new_val] = {str_id: ""}
        
        self.save_indices()
        
        with open(f"{self.name}.csv", 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if line.startswith(f"{str_id},"):
                    row = line.strip().split(',')
                    for col, val in values.items():
                        index = list(self.schema.keys()).index(col) + 1
                        row[index] = str(val)
                    f.write(','.join(row) + '\n')
                else:
                    f.write(line)
            f.truncate()



    def query(self, **conditions):
        results = []
        candidate_ids = set(range(1, self.auto_id+1))
        for col, value in conditions.items():
            if col not in self.schema:
                raise Exception("Invalid column in conditions")
            ids_for_value = set(map(int, self.column_indices[col].get(value, {}).keys()))
            candidate_ids.intersection_update(ids_for_value)
        
        with open(f"{self.name}.csv", 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if int(row[0]) in candidate_ids:
                    results.append(OrderedDict(zip(["id"] + list(self.schema.keys()), row)))

        return results

    def load(self):
        if os.path.exists(f"{self.name}_schema.json"):
            with open(f"{self.name}_schema.json", 'r') as f:
                self.schema = json.load(f)

        for col in self.schema.keys():
            index_file = f"{self.name}_{col}_index.json"
            if os.path.exists(index_file):
                with open(index_file, 'r') as f:
                    self.column_indices[col] = self.convert_index_keys_to_given_data_type(col, json.load(f))

        if os.path.exists(f"{self.name}.csv"):
            with open(f"{self.name}.csv", 'r') as f:
                lines = f.readlines()
                if lines:
                    self.auto_id = int(lines[-1].split(",")[0])

    def convert_index_keys_to_given_data_type(self, col, dictt):
        if self.schema[col] == "int":
            return {int(k): v for k, v in dictt.items()}
        elif self.schema[col] == "float":
            return {float(k): v for k, v in dictt.items()}
        elif self.schema[col] == "bool":
            return {bool(k): v for k, v in dictt.items()}
        else:
            return dictt
        
    def convert_col_value_to_given_data_type(self, col, value):
        if self.schema[col] == "int":
            return int(value)
        elif self.schema[col] == "float":
            return float(value)
        elif self.schema[col] == "bool":
            return bool(value)
        else:
            return value


    def row_exists(self, id):
        return id <= self.auto_id and id > 0

    def get_row_by_id(self, id):
        with open(f"{self.name}.csv", 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if int(row[0]) == id:
                    return row

    def save_indices(self):
        for col, index_data in self.column_indices.items():
            with open(f"{self.name}_{col}_index.json", 'w') as f:
                json.dump(index_data, f)

    def print_table(self):
        with open(f"{self.name}.csv", 'r') as f:
            print(f.read())

if __name__ == '__main__':
    db = FlatFileDB('my_table')
    db.load()
    db.create_table(name="str", age="int")

    db.insert(name='Alice', age=30)
    db.insert(name='Bob', age=40)
    db.insert(name='Charlie', age=50)
    
    print("Before Delete:")
    db.print_table()

    db.delete(2)

    print("After Delete:")
    db.print_table()

    db.update(1, name='Alicia', age=35)

    print("After Update:")
    db.print_table()

    print("Query Result:")
    print(db.query(age=50))
