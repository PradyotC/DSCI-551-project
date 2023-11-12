import cli_parser as cparse
import json

if __name__ == "__main__":
    while True:
        query = input("myql> ")
        if query in ("quit", "exit", "^Z"):
            break
        else:
            print(json.dumps(cparse.parse_sql(query), indent=4))
