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
    level = 2

    package_id = '234221136'

    tool_id = '11234221128'

    # This prevents sql injection for the order by clause. Never use data sent by the user directly in a query
    actual_order_by = 'name'
    if order == 'name':
        actual_order_by = 'name'
    if order == 'description':
        actual_order_by = 'description'
    if order == 'created_on':
        actual_order_by = 'created_on'

    offset = page * limit     

    try:
        # Use getconn() method to get Connection from connection pool
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        path = '/packages'

        directory_path = '/home/ubuntu/efs-mount-point/home/cadre-query-results/m52xa5dbmfsgs' + path
        
        file_info = []
        for root, dirs, files in os.walk(directory_path):
            _root = root.replace(directory_path, '')
            if _root.count(os.sep) < level:
                for file_name in files:
                    print(os.path.join(root, file_name))
                    file_info.append({'path': '{}'.format(os.path.join(root, file_name)), 'type': 'file'})
                for directory_name in dirs:
                    print(os.path.join(root, directory_name))
                    file_info.append({'path': '{}'.format(os.path.join(root, directory_name)), 'type': 'folder'})


        # Here we are printing the value of the list
        for x in range(len(file_info)):
            print(file_info[x])

        files_response = json.dumps(file_info)
        print(files_response)

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
