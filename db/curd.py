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

    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def execute_commit_query(self, query, params=None):
        """Execute the SQL query and commit."""
        self.connect()
        self.cursor.execute(query, params)
        self.commit()
        self.close()

    def execute_query(self, query, limit=None, offset=None):
        """Execute the SQL query."""
        self.connect()
        if limit is not None and offset is not None:
            self.cursor.execute(query, (limit, offset))
        else:
            self.cursor.execute(query)
        result = self.cursor.fetchall()
        self.close()
        return result

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

    def get_one(self, query, params=None):
        """Return a single row as a dictionary."""
        self.connect()
        self.cursor.execute(query, params)
        columns = [col[0] for col in self.cursor.description]
        result = dict(zip(columns, self.cursor.fetchone()))
        self.close()
        return result

    def check_if_exists(self, query, params=None):
        """Check if a record exists in the database."""
        self.connect()
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        self.close()
        return result


class DataHandler(DataRetriever):
    """Handles insertion and updates for onboarding tables."""

    def insert_into_contentonly(self, data):
        query = """
        INSERT INTO Onboarding.ContentOnly (
            sku, product_name, category_name, brand_name, description,
            meta_title, meta_description, meta_keywords, product_id,
            category_id, brand_id, variant_id, option_id, option_parent_id
        ) VALUES (
            %(sku)s, %(product_name)s, %(category_name)s, %(brand_name)s,
            %(description)s, %(meta_title)s, %(meta_description)s,
            %(meta_keywords)s, %(product_id)s, %(category_id)s,
            %(brand_id)s, %(variant_id)s, %(option_id)s, %(option_parent_id)s
        )
        ON CONFLICT (sku) DO UPDATE SET
            description = EXCLUDED.description,
            meta_title = EXCLUDED.meta_title,
            meta_description = EXCLUDED.meta_description,
            meta_keywords = EXCLUDED.meta_keywords;
        """
        self.execute_commit_query(query, data)

    def insert_into_optionsmerging(self, data):
        query = """
        INSERT INTO Onboarding.OptionsMerging (
            sku, type, size, weight, qty, color, style, material
        ) VALUES (
            %(sku)s, %(type)s, %(size)s, %(weight)s, %(qty)s,
            %(color)s, %(style)s, %(material)s
        )
        ON CONFLICT (sku) DO UPDATE SET
            type = EXCLUDED.type,
            size = EXCLUDED.size,
            weight = EXCLUDED.weight,
            qty = EXCLUDED.qty,
            color = EXCLUDED.color,
            style = EXCLUDED.style,
            material = EXCLUDED.material;
        """
        self.execute_commit_query(query, data)
