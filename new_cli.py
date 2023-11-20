import cmd
from SimpleTableManager import SimpleDB # Import the SimpleDB class from the module you saved it in
import re


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


def parse_where_clause(where_clause):
    operator = None
    if ' AND ' in where_clause:
        operator = 'AND'
        conditions = where_clause.split(' AND ')
    elif ' OR ' in where_clause:
        operator = 'OR'
        conditions = where_clause.split(' OR ')
    else:
        operator = None
        conditions = [where_clause]

    parsed_conditions = []
    for condition in conditions:
        match = re.match(r'(.*?)\s*([=<>!]{1,2})\s*(.*)', condition)
        if match:
            column_name, condition_operator, value = match.groups()
            parsed_conditions.append((column_name, condition_operator, value))

    return parsed_conditions, operator

def parse_conditions(where_clause):
    match = re.match(r'(.*?)\s*([=<>!]{1,2})\s*(.*)', where_clause)
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
    return tks_stack[::-1]

    

class SimpleDBCLI(cmd.Cmd):
    intro = 'Welcome to the SimpleDB shell. Type help or ? to list commands.\n'
    prompt = 'myDB> '

    def __init__(self):
        super().__init__()
        self.db = SimpleDB()

    def do_create(self, arg):
        'Create a new table: CREATE TABLE_NAME COLUMN1_TYPE COLUMN2_TYPE ... PRIMARY_KEY'
        args = arg.split()
        if len(args) < 3:
            print("Invalid syntax. Correct syntax: CREATE TABLE_NAME COLUMN1_TYPE COLUMN2_TYPE ... PRIMARY_KEY")
            return
        table_name = args[0]
        primary_key = args[-1]
        columns = {args[i]: args[i + 1] for i in range(1, len(args) - 1, 2)}
        try:
            self.db.create_table(table_name, columns, primary_key)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def do_insert(self, arg):
        'Add an entry to a table: INSERT TABLE_NAME COLUMN1_VALUE COLUMN2_VALUE ...'
        args = arg.split()
        if len(args) < 3:
            print("Invalid syntax. Correct syntax: INSERT TABLE_NAME COLUMN1_VALUE COLUMN2_VALUE ...")
            return
        table_name = args[0]
        print(args[1:])
        columns = self.db._load_json_file(f"data/{table_name}/{table_name}_schema.json")["columns"]
        if len(args[1:]) != len(columns):
            print(f"Expected {len(columns)} values, got {len(args[1:])}.")
            return
        values = {column: args[i+1] for i, column in enumerate(columns)}
        print(values)
        try:
            self.db.insert(table_name, values)
            print(f"Entry added to table {table_name}.")
        except Exception as e:
            print(f"Error inserting into table: {e}")

    def do_delete(self, arg):
        'Delete an entry from a table: DELETE TABLE_NAME PRIMARY_KEY=PRIMARY_KEY_VALUE'
        args = arg.split()
        if len(args) != 2:
            print("Invalid syntax. Correct syntax: DELETE TABLE_NAME PRIMARY_KEY=PRIMARY_KEY_VALUE")
            return
        table_name = args[0]
        primary_key, primary_key_value = args[1].split("=")
        try:
            self.db.delete(table_name, primary_key, primary_key_value)
            print(f"Entry deleted from table {table_name}.")
        except Exception as e:
            print(f"Error deleting from table: {e}")


    def do_select(self, arg):
        'Select entries from a table: SELECT TABLE_NAME PRIMARY_KEY=PRIMARY_KEY_VALUE or SELECT TABLE_NAME COLUMN1 COLUMN2 ...'
        args = arg.split()
        if len(args) < 1:
            print("Invalid syntax. Correct syntax: SELECT TABLE_NAME PRIMARY_KEY=PRIMARY_KEY_VALUE or SELECT TABLE_NAME COLUMN1 COLUMN2 ...")
            return
        table_name = args[0]
        if len(args) == 1:
            try:
                keys,entries = self.db.execute_query(table_name)
                print_data(keys,entries)
            except Exception as e:
                print(f"Error selecting from table: {e}")
        elif "where" not in args:
            columns = args[1:]
            try:
                keys,entries = self.db.execute_query(table_name, columns)
                print_data(keys,entries)
            except Exception as e:
                print(f"Error selecting from table: {e}")
        else:
            where_index = args.index("where")
            columns = args[1:where_index]
            if not columns:
                columns = None
            conditions = parse_conditions(" ".join(args[where_index + 1:]))
            try:
                keys,entries = self.db.execute_query(table_name, columns, conditions)
                print_data(keys,entries)
            except Exception as e:
                print(f"Error selecting from table: {e}")
            # conditions,operator = parse_where_clause(" ".join(args[where_index + 1:]))
            # try:
            #     keys,entries = self.db.select(table_name, columns, conditions, operator)
            #     print_data(keys,entries)
            # except Exception as e:
            #     print(f"Error selecting from table: {e}")
    

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
