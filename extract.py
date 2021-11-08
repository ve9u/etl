import pymysql
import config

def initalise_mysql():
    """Initalises and returns a MySQL database based on config"""
    return pymysql.connect(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USERNAME,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB)

def extract_data(mysql_cursor):
    """Given a cursor, Extracts data from MySQL classic product dataset
    and returns all the tables with their data"""
    genres = execute_mysql_query('select * from genres', mysql_cursor, 'fetchall')
    movie_genres = execute_mysql_query('select * from movie_genres', mysql_cursor, 'fetchall')
    movies = execute_mysql_query('select * from movies', mysql_cursor, 'fetchall')
    ratings = execute_mysql_query('select * from ratings', mysql_cursor, 'fetchall')
    users = execute_mysql_query('select * from users', mysql_cursor, 'fetchall')
    tables = (genres, movie_genres, ratings, users, movies)
    return tables
