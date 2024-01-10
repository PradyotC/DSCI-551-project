import cmd
from SimpleTableManager import SimpleDB # Import the SimpleDB class from the module you saved it in
import re
import shutil
import os


def print_data(keys,data):
    print("\n\n")
    print("-" * (len(keys) * 3))
    print(" | ".join(keys))
    print("-" * (len(keys) * 3))
    for row in data:
        print(" | ".join([str(x) for x in row]))

    print("\n\n")
    print("\n{} rows.".format(len(data)))
    print("\n\n")

def parse_conditions(where_clause):
    if 'ORDER_BY' in where_clause:
        where_clause, order_by = where_clause.split('ORDER_BY')
        order_by = order_by.split()
    else:
        order_by = None
    # match = re.match(r'(.*?)\s*([=<>!]{1,2})\s*(.*)', where_clause)
    tokens = re.split(r'(\(|\)|AND|OR|==|>|<|!=|<=|>=)', where_clause)
    tokens = [token.strip() for token in tokens if token.strip()]

    tks = tokens.copy()
    tks_stack = []
    while tks:
        token = tks.pop()
        if token == ')':
            temp_stack = []
            while len(tks)>1 and tks[-1] != '(':
                temp_stack.append(tks.pop())
            tks_stack.append(temp_stack[::-1])
            tks.pop()
        else:
            tks_stack.append(token)
    return None if not tks_stack else tks_stack[::-1], order_by

    

class SimpleDBCLI(cmd.Cmd):
    intro = 'Welcome to the SimpleDB shell. Type help or ? to list commands.\n'
    prompt = 'myDB> '

    aggregate_functions = {
        "SUM": 4,
        "AVG": 4,
        "COUNT": 6,
        "MIN": 4,
        "MAX": 4
    }


    def __init__(self):
        super().__init__()
        self.db = SimpleDB()


    def do_show(self, arg):
        'Show all tables: show tables'
        args = arg.split()
        if len(args) < 1 or args[0] != "tables":
            print("Invalid syntax. Correct syntax: show tables")
            return
        datapath = os.path.join(os.getcwd(), "data")
        if os.path.exists(datapath):
            tables = os.listdir(datapath)
            print("\nTABLES\n-------\n")
            if ".DS_Store" in tables:
                tables.remove(".DS_Store")
            for table in tables:
                print(table)
        else:
            print("No tables found.")
        print("\n")

    def do_schema(self, arg):
        'Show schema of a table: schema TABLE_NAME'
        args = arg.split()
        if len(args) < 1:
            print("Invalid syntax. Correct syntax: schema TABLE_NAME")
            return
        table_name = args[0]
        table_path = os.path.join(os.getcwd(), "data", f"{table_name}")
        if not os.path.exists(table_path):
            print("Table does not exist.")
            return
        schema_path = os.path.join(table_path, f"{table_name}_schema.json")
        schema = self.db._load_json_file(schema_path)
        table_columns = schema["columns"]
        print("\nSCHEMA\n-------\n")
        for column_name,column_datatype in table_columns.items():
            print(f"{column_name} | {column_datatype}")
        print("\n")


    def do_make(self, arg):
        'Make a table: make table TABLE_NAME where COLUMN1_TYPE=COLUMN2_TYPE, ..., PRIMARY_KEY=PRIMARY_KEY_COLUMN'
        args = arg.split()
        if len(args) < 2:
            print("Invalid syntax. Correct syntax: make table TABLE_NAME where COLUMN1_TYPE=COLUMN2_TYPE, ..., PRIMARY_KEY=PRIMARY_KEY_COLUMN")
            return
        table_name = args[1]
        primary_key = args[-1].split("=")[1].strip()
        columns = "".join(args[3:-1]).split(",")
        columns = {col.strip().split("=")[0]: col.strip().split("=")[1] for col in columns if col.strip()}
        try:
            self.db.create_table(table_name, columns, primary_key)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def do_add(self, arg):
        'Add an entry to a table: add an entry to TABLE_NAME with COLUMN1=COLUMN1_VALUE , COLUMN2=COLUMN2_VALUE ...'
        insert_pattern = r"an\s+entry\s+to\s+(\w+)\s+with\s+(.*)"
        try:
            match = re.match(insert_pattern, arg, re.IGNORECASE)

            if not match:
                raise ValueError("Invalid insert query")
        
            table_name, values = match.group(1), match.group(2).split(",")
            value_list = [val.strip().split("=") for val in values]
            ret =  {
            "table_name": table_name,
            "values": {val[0]: val[1] for val in value_list},
            }
            self.db.insert(table_name, ret["values"])
            print(f"Entry added to table {table_name}.")
            
        
        except Exception as e:
            print(f"Error inserting into table: {e}")

    def do_delete(self, arg):
        'Delete an entry from a table: delete entry with PRIMARY_KEY=PRIMARY_KEY_VALUE from TABLE_NAME'
        try:

            delete_pattern = r"entry\s+with\s+(.*)\s+from\s+(\w+)"
            match = re.match(delete_pattern, arg, re.IGNORECASE)

            if not match:
                raise ValueError("Invalid delete query")

            condition, table_name = match.group(1), match.group(2)

            col_name,col_value = condition.split("=")
            self.db.delete(table_name, col_name, col_value)
            print(f"Entry deleted from table {table_name}.")
        except Exception as e:
            print(f"Error deleting from table: {e}")


    def do_drop(self, arg):
        "Drop a table: drop table TABLE_NAME"
        args = arg.split()
        try:
            if args[0] not in ["table", "TABLE"]:
                raise ValueError("Invalid drop query")
            table_name = args[1]
            table_path = os.path.join(os.getcwd(), "data", f"{table_name}")
            if not os.path.exists(table_path):
                raise ValueError("Table does not exist")
            shutil.rmtree(table_path)
            print(f"Table {table_name} dropped successfully.")
        except Exception as e:
            print(f"Error dropping table: {e}")

    def do_modify(self, arg):
        "Modify an entry in a table: modify entry with PRIMARY_KEY=PRIMARY_KEY_VALUE from TABLE_NAME with COLUMN1=COLUMN1_VALUE , COLUMN2=COLUMN2_VALUE ..."
        bol = True
        try:
            args = arg.split()
            primary_key_data = args[2].split("=")
            primary_key_col,primary_key_value = primary_key_data[0].strip(),primary_key_data[1].strip()
            table_name = args[4].strip()
            values = "".join(args[6:]).split(",")
            values = {val.strip().split("=")[0]: val.strip().split("=")[1] for val in values if val.strip()}
            if not table_name or not primary_key_col or not primary_key_value or not values:
                bol = False
                raise ValueError("Invalid modify query")
            else:
                values[primary_key_col] = primary_key_value
        except Exception as e:
            bol = False
            print("Invalid modify query")
        if bol:
            try:
                self.db.update(table_name, primary_key_col, primary_key_value, values)
                print(f"Entry updated in table {table_name}.")
            except Exception as e:
                print(f"Error updating table: {e}")
    

    def do_get(self, arg):
        """Get an entry from a table: get entry (column1,column2,....) with PRIMARY_KEY=PRIMARY_KEY_VALUE from TABLE_NAME
        optional: get entry (column1,column2,....) with PRIMARY_KEY=PRIMARY_KEY_VALUE from TABLE_NAME sorted_by COLUMN_NAME ASC/DESC
        optional: get entry (column1,column2,....) with PRIMARY_KEY=PRIMARY_KEY_VALUE from TABLE_NAME when COLUMN_NAME OPERATOR VALUE and COLUMN_NAME OPERATOR VALUE sorted_by COLUMN_NAME ASC/DESC"""
        args = arg.split()
        table_name, columns, join_table_name, join_condition, conditions, order_by, group_by = None, None, None, None, None, None, None
        bol = True
        try:
            idx = 0
            while idx < len(args):
                if args[idx] == 'from':
                    columns = "".join(args[0:idx])
                    table_name = args[idx + 1]
                    break
                idx += 1
            if not columns: columns = None
            if "merged_with" in args:
                idx = args.index("merged_with")
                while idx < len(args):
                    if args[idx] == 'merged_with':
                        join_table_name = args[idx + 1]
                        join_condition = args[idx + 3]
                        break
                    idx += 1
            last_idx = len(args)
            if "group_by" in args:
                idx = args.index("group_by")
                last_idx = min(idx, last_idx)
                group_by = args[idx + 1]
            if "sorted_by" in args:
                idx = args.index("sorted_by")
                last_idx = min(idx, last_idx)
                order_by = [args[idx + 1], args[idx + 2]]
            if "when" in args:
                idx = args.index("when")
                conditions = " ".join(args[idx + 1:last_idx])
            if columns:
                columns = columns.split(",")
                columns = [column.strip() for column in columns if column.strip()]
            if conditions:
                conditions,_ = parse_conditions(conditions)
        except Exception as e:
            bol = False
            print("Invalid query")
        if bol:
            try:
                if not group_by:
                    keys,entries = self.db.execute_join_query(table_name, columns, conditions, order_by, join_table_name, join_condition)
                    print_data(keys,entries)
                elif group_by and columns:
                    aggregate_info = columns[1].split('(')  # Splitting to get aggregate function and column
                    aggregate_func = aggregate_info[0].upper()  # Getting the aggregate function (SUM, MAX, MIN, COUNT)
                    aggregate_col = aggregate_info[1][:-1] if aggregate_func != "COUNT" else None  # Getting the column for aggregation
                    keys,entries = self.db.perform_group_by(table_name, columns[0], aggregate_func, aggregate_col)
                    print_data(keys,entries)
                    if os.path.exists("temp_merged_0.csv"):
                        os.system("rm temp_merged_*.csv")
                else:
                    raise ValueError("Invalid Group By Query")
            except Exception as e:
                print(f"Error selecting from table: {e}")
            finally:
                if os.path.exists("temp_merged_0.csv"):
                    os.system("rm temp_merged_*.csv")
        


    def do_quit(self, arg):
        'Exit the SimpleDB shell.'
        print("Exiting SimpleDB shell.")
        return True  # Return True to exit the cmd loop

    def do_exit(self, arg):
        'Exit the SimpleDB shell.'
        print("Exiting SimpleDB shell.")
        return True

    def do_clear(self, arg):
        'Clear the screen.'
        print("\033[H\033[J")

    "get previous command on up arrow key press"
    

    def default(self, line):
        print(f"Unknown command: {line}")

# Run the command loop
if __name__ == '__main__':
    SimpleDBCLI().cmdloop()