import os
import mysql.connector

def get_db_connection():
    """Return a connection to the database."""
    
    return mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME', 'PAM')
    )