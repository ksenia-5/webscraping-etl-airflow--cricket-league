import sqlite3
from sqlite3 import Error
import os

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def query_batter_data(con) -> None:
    cur = con.cursor()
    cur.execute("   SELECT DISTINCT BatterName FROM scores ORDER BY BatterName;")
    result = [n[0] for n in cur.fetchall()]
    print(result)
    con.commit()

    

def main():
    database = 'sql/cricket.db'

    conn = create_connection(database)
    
    if conn is not None:
        query_batter_data(conn)
        
        conn.close()
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()