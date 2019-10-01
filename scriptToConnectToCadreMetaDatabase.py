import psycopg2
from psycopg2 import pool
import json
import os, sys
import collections
import logging
import metadatabase_config

# cadre metadatabase settings
db_host = metadatabase_config.database_host
db_port = metadatabase_config.database_port
db_username = metadatabase_config.database_username
db_password = metadatabase_config.database_password
db_name = metadatabase_config.database_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    connection_pool = pool.SimpleConnectionPool(1,
                                                20,
                                                host=db_host,
                                                database=db_name,
                                                user=db_username,
                                                password=db_password,
                                                port=db_port)
except:
    logger.error("ERROR: Unexpected error: Could not connect to the Postgres instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")


def check_database_query():
    """
    This function checks whether the database query is working as intended
    """
    limit = 25
    page = 0
    order = 'name'

    offset = page * limit

    try:
        # Use getconn() method to get Connection from connection pool
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        # Print PostgreSQL version
        cursor.execute("SELECT archive_id, s3_location, description as archive_description, name as archive_name, trim(both '\"' from to_json(created_on)::text) as archive_created_on FROM archive ORDER BY {} LIMIT {} OFFSET {};".format(order, limit, offset))
        if cursor.rowcount > 0:
          #  for query in cursor:
              #  print(str(query))
           archive_info = cursor.fetchall()
           archive_list = []
           for archives in archive_info:
              archive_json = {
                 'archive_id': archives[0],
                 's3_location': archives[1],
                 'archive_description': archives[2],
                 'archive_name': archives[3],
                 'archive_created_on': archives[4]
              }  
              archive_list.append(archive_json)
          # print(archive_list)   
           print(json.dumps(archive_list))

    except Exception:
        return ({"Error:", "Problem querying the package table or the archive table or the tools table in the meta database."}), 500

    finally:
        # Closing database connection.
        cursor.close()
        # Use this method to release the connection object and send back to the connection pool
        connection_pool.putconn(connection)
        print("PostgreSQL connection pool is closed")


if __name__ == '__main__':
    check_database_query()
