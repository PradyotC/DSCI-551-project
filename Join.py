import re
import os
import csv
import json

def process_command(command):
    cmd_parts = command.split(maxsplit=2)
    print(cmd_parts)
    cmd_type = cmd_parts[0]          

    if cmd_type == "create_table":
        return create_table_command(cmd_parts)
    elif cmd_type == "insert_into":
        return insert_into_command(cmd_parts)
    elif cmd_type == "select":
        return select_command(cmd_parts)
    else:
        return "Unknown command"

def create_table_command(cmd_parts):
    #Eg: create_table student id,name,age
    if len(cmd_parts) < 3:
        return "Invalid create_table command format"
    filename, columns = cmd_parts[1], cmd_parts[2]
    columns = columns.split(",")
    if not filename.endswith('.csv'):
        filename += '.csv'

    # Check if the file already exists
    if os.path.exists(filename):
        return f"Table {filename} already exists."

    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
    return f"Table {filename} created with columns {', '.join(columns)}."

def insert_into_command(cmd_parts):
    #Eg: insert_into student id=1,name=Bhaven,age=25
    # insert_into student id=2,name=Prads,age=24
    if len(cmd_parts) != 3:
        return "Invalid insert_into command format"
    filename, column_data = cmd_parts[1], cmd_parts[2]
    if not filename.endswith('.csv'):
        filename += '.csv'

    # Parsing column data
    data = {}
    for pair in column_data.split(','):
        key, value = pair.split('=')
        data[key.strip()] = value.strip().strip('"')

    # Read the header to maintain the column order
    with open(filename, 'r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)

    # Map the data to the column order in the header
    ordered_data = {column: data.get(column, '') for column in header}

    # Write the ordered data
    with open(filename, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writerow(ordered_data)
        # update_meta_file(filename, increment=True)

    return f"Values inserted into {filename}."

def select_command(cmd_parts):
    #Eg: select id,name from student where id==2
    print(cmd_parts)
    if len(cmd_parts) < 2:
        return "Invalid select command format"
    
    command_str = ' '.join(cmd_parts[1:])
    try:
        columns_part, rest = command_str.split(' from ')
        columns = [col.strip() for col in columns_part.split(',')]
        if "join" in rest:
            table1,on_part = rest.split(' join ')
            table2,condition = on_part.split(' on ')
            perform_join(columns,table1,table2,condition,columns)
            return "Join query executed."

        elif "where" in rest:
            filename, where_clause = rest.split(' where ')
            conditions,order_by = parse_conditions(where_clause)
            chunk_size = 10
            if not filename.endswith('.csv'):
                filename += '.csv'

            execute_query(filename, columns, conditions, order_by, chunk_size)
        else:
            filename = rest
            if not filename.endswith('.csv'):
                filename += '.csv'

            # Check if the file already exists
            if os.path.exists(filename):
                return f"Table {filename} already exists."

            execute_query(filename, columns)

    except ValueError:
        return "Invalid select command format. Ensure you use 'select col1, col2 from filename where condition'."

    meta_file = 'meta.json'
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as file:
            meta_data = json.load(file)
            table_lines = meta_data.get(filename, 0)
            chunk_line_count = max(1, table_lines // 5)
    else:
        chunk_line_count = 1  # Default value if meta file doesn't exist
    print("Chunk line count:", chunk_line_count)
    # process_file_in_chunks(filename, columns, chunk_line_count, conditions, logical_operator)

    return f"Select query executed on {filename}."

def perform_join(columns,table1,table2,condition,fields,chunk_line_count=2):
    #eg: select id,name from student.csv join employees.csv on student.id==employees.id
    #eg: select id,name from employees.csv join employees.csv on employees.id==employees.id
    print("Join")
    condition = condition.split('==')  # Assuming condition is like 'student.id==employees.id'
    table1_key = condition[0].split('.')[-1]  # Extracting key for table1
    table2_key = condition[1].split('.')[-1]  # Extracting key for table2
    print(table1_key, table2_key)
    join_file = table1.split('.')[0] + '_' + table2.split('.')[0] + '.csv'
    delete_file_if_exists(join_file)

    with open(table1, 'r', newline='') as file1:
        reader1 = csv.DictReader(file1)
        for chunk1 in chunk_reader(reader1, chunk_line_count):
            with open(table2, 'r', newline='') as file2:
                reader2 = csv.DictReader(file2)
                for chunk2 in chunk_reader(reader2, chunk_line_count):
                    for row1 in chunk1:
                        for row2 in chunk2:
                            if row1[table1_key] == row2[table2_key]:
                                joined_row = prefix_row_keys(row1, table1) | prefix_row_keys(row2, table2)
                                write_to_csv(joined_row, join_file)
                                # print(joined_row)

def write_to_csv(row, filename):
    with open(filename, 'a', newline='') as file:
        fieldnames = row.keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if file.tell() == 0:  # Check if the file is empty to write header
            writer.writeheader()

        writer.writerow(row)

def delete_file_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted existing file: {file_path}")

def get_prefixed_fieldnames(fieldnames, prefix):
    return [f"{prefix}.{fieldname}" for fieldname in fieldnames]

def prefix_row_keys(row, prefix):
    return {f"{prefix}.{k}": v for k, v in row.items()}

def chunk_reader(reader, chunk_size):
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def parse_conditions(where_clause):
    # print("Where",where_clause)
    "(department==HR AND salary>60000) AND (id==146 OR id==102) ORDER_BY salary DESC"
    '''split ORDER_BY if exists'''
    if 'ORDER_BY' in where_clause:
        where_clause, order_by = where_clause.split('ORDER_BY')
        order_by = order_by.split()
    else:
        order_by = None
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
    print(tks_stack[::-1])
    return None if not tks_stack else tks_stack[::-1], order_by

def execute_query(filename,fields=None, conditions=None,order_by=None, chunk_line_count=10):

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        selected_columns = [col for col in fields if col in reader.fieldnames]

        chunk_count, chunk,result = 0, [],[]
        for row in reader:
            chunk.append(row)

            if len(chunk) >= chunk_line_count:
                filtered_chunk = [r for r in chunk if not conditions or evaluate_conditions(r, conditions)]
                result.extend(process_chunk(filtered_chunk, chunk_count, selected_columns))
                chunk, chunk_count = [], chunk_count + 1

        if chunk:  # Process last chunk
            filtered_chunk = [r for r in chunk if not conditions or evaluate_conditions(r, conditions)]
            result.extend(process_chunk(filtered_chunk, chunk_count, selected_columns))
    print(result)

def process_chunk(chunk, chunk_count, selected_columns):
    # print(f"Chunk {chunk_count}:")
    ans = []
    for row in chunk:
        ans.append(row)
    return ans

def evaluate_condition(item, condition):
    field, operator, value = condition
    field = field.strip('()')
    value = value.strip('()')
    # print(field, operator, value)
    if operator == '>':
        return int(item[field]) > int(value)
    elif operator == '<':
        return int(item[field]) < int(value)
    elif operator == '>=':
        return int(item[field]) >= int(value)
    elif operator == '<=':
        return int(item[field]) <= int(value)
    elif operator == '==':
        return str(item[field]) == value
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def evaluate_conditions(item, conditions):
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

        return evaluate_condition(item, conds)

    return recursive_eval(conditions)

while True:
    command = input("MyDB > ")
    if command == "exit":
        break
    response = process_command(command)
    print(response)