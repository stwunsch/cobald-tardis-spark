import argparse
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("path", help="Path to the database.")
parser.add_argument("--create", help="Create the drones db from scratch.", action="store_true")
parser.add_argument("--print", help="Print the drones db.", action="store_true")
args = parser.parse_args()

def create_db():
    sqliteConnection = sqlite3.connect(args.path)
    sqlite_create_table_query = """CREATE TABLE yarn_drones (
                                drone_uuid TEXT PRIMARY KEY,
                                nm TEXT NOT NULL,
                                status TEXT NOT NULL);"""
    cursor = sqliteConnection.cursor()
    cursor.execute(sqlite_create_table_query)
    sqliteConnection.commit()
    print("SQLite table created")

    cursor.close()
    sqliteConnection.close()
    print("sqlite connection is closed")

def print_db():
    sqliteConnection = sqlite3.connect(args.path)
    with sqliteConnection:
        cursor = sqliteConnection.cursor()
        cursor.execute("SELECT * FROM yarn_drones")
        for line in cursor.fetchall():
            print(line)

if __name__ == "__main__":
    if args.create:
        create_db()
    elif args.print:
        print_db()

