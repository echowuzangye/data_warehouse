import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from s3 bucket to staging tables using the queries in `copy_table_queries`.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transforms and loads data from staging tables to fact and dimension tables using the queries in `insert_table_queries`.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Loads DWH Params from dwh.cfg file
    - Connects to the cluster
    - Loads data from s3 to staging tables using the function defined above: load_staging_tables  
    - Creates fact and dimension tables using the function defined above: insert_tables  
    - Finally, closes the connection. 
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