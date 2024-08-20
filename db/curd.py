from db import connect_to_postgres, close_connection
from app.config import Config


class DataRetriever:
    """Class to retrieve data from a PostgreSQL database."""

    db_name = Config.DB_NAME
    db_user = Config.DB_USER
    db_pwd = Config.DB_PASSWORD
    db_host = Config.DB_HOST
    db_port = Config.DB_PORT

    def __init__(self, database=db_name,
                 user=db_user, password=db_pwd,
                 host=db_host, port=5432, schema='public'):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to the database."""
        self.conn, self.cursor = connect_to_postgres(
            self.database, self.user, self.password, self.host, self.port, self.schema)

    def close(self):
        """Close the database connection."""
        close_connection(self.conn, self.cursor)

    def execute_query(self, query, limit=None, offset=None):
        """Execute the SQL query."""

        if limit is not None and offset is not None:
            self.cursor.execute(query, (limit, offset))
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def query(self, query, limit=None, offset=None):
        """Return the data as a list of dictionaries."""
        self.connect()
        if limit is not None and offset is not None:
            data = self.execute_query(query, limit, offset)
        else:
            data = self.execute_query(query)
        columns = [col[0] for col in self.cursor.description]
        result = [dict(zip(columns, row)) for row in data]
        self.close()
        return result

    def get_one(self, query):
        """Return a single row as a dictionary."""
        self.connect()
        self.cursor.execute(query)
        columns = [col[0] for col in self.cursor.description]
        result = dict(zip(columns, self.cursor.fetchone()))
        self.close()
        return result
