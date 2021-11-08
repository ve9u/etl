import pymysql

from config import MYSQL_DB, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_USERNAME

def connect_db():
    # Connect to the database
    connection = pymysql.connect(host=MYSQL_HOST,
                                user=MYSQL_USERNAME,
                                password=MYSQL_PASSWORD,
                                database=MYSQL_DB,
                                cursorclass=pymysql.cursors.DictCursor)
    return connection

def extract_data(connection, sql):
    with connection.cursor() as cursor:
        # Read a single record
        cursor.execute(sql)
        return cursor
