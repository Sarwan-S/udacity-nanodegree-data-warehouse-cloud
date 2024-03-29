import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Function to run query to copy songs and events data in S3 into staging tables
    Inputs: 
        cur: cursor for database insertions
        conn: database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: Function to run sql queries that performs etl and inserts data from staging tables into fact and dimension tables
    Inputs: 
        cur: cursor for database insertions
        conn: database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: main function for running both the load_staging_tables and insert_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()