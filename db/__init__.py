import os
import mysql.connector
import psycopg2

def get_db_connection():
    """Return a connection to the database."""
    
    return mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME', 'PAM')
    )



def connect_to_postgres(dbname, user, password, host, port, schema):
    try:
        # Establishing a connection to the database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        
        # Setting the search_path to the specified schema
        cursor.execute("SET search_path TO %s;", (schema,))

        # Executing a SQL query
        cursor.execute("SELECT version();")

        # Fetching the result
        # record = cursor.fetchone()
        # print("You are connected to - ", record)

        return conn, cursor

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None, None

def close_connection(conn, cursor):
    # Closing database connection.
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
