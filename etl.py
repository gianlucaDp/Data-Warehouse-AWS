import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function connects to the database and calls the functions to process
    the copy queries
    Arguments:
        None
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function connects to the database and calls the functions to process
    the insert queries
    Arguments:
        None
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
    The function executes the following steps
    - Connects to a redshift cluster
    
    - Loads all the data from the S3 buckets to the staging tables
    
    - Inserts all the data into the dimensionals and fact table from the staging tables
    
    - Finally, closes the connection.
    
    Returns: None
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
