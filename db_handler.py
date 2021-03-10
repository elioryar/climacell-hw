import mysql.connector


class DBhandler:
    def __init__(self):
        self.db_connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        self.db_connection = mysql.connector.connect(
            host='test-db.crltxdtvqzqv.eu-central-1.rds.amazonaws.com',
            port=3306,
            user='elioryarkoni5',
            password='elioryarkoni5',
            database='climacell_db',
            allow_local_infile=True
        )

        self.cursor = self.db_connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


db_connection = DBhandler()
