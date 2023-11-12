from database import FlatFileDB

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
