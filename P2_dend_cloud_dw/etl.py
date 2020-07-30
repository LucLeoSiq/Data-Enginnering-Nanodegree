import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    COPY commands (in 'sql_queries') loads data from S3 Bucket to staging area (tables: staging_events, staging_songs)
    :param cur: Cursor object. Allows python code to execute PostgreSQL in a database session
    :param conn: Connection to the data warehouse. It encapsulate the database session
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes insert queries in `sql_queries` (inserts into: songplay, user, song, artist, time) 
    :param cur: Cursor object. Allows python code to execute PostgreSQL in a database session
    :param conn: Connection to the data warehouse. It encapsulate the database session
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # configuration object
    config = configparser.ConfigParser()
    # configuration file `dwh.cfg`
    config.read('dwh.cfg')

    # obtain a database session through connection object
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # obtain cursor object to execute queries
    cur = conn.cursor()

    # load data from s3 buckets to staging 
    load_staging_tables(cur, conn)
    # Moves data from staging area to model
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()