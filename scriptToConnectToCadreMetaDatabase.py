import psycopg2
from psycopg2 import pool
import json
import os, sys
import collections
import logging
import metadatabase_config
from datetime import date

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


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


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
  
        query = ("""SELECT 
                    tool_id, 
                    tool.description as tool_description, 
                    tool.name as tool_name, 
                    tool.script_name as tool_script_name, 
                    tool.created_on as tool_created_on
                FROM tool 
                WHERE tool_id = '%s';""" % (tool_id))

        cursor.execute(query)
        
        if cursor.rowcount > 0:
            tool_info = cursor.fetchall()
            tool_list = []
            for tools in tool_info:
                tool_json = {
                    'tool_id': tools[0],
                    'tool_description': tools[1],
                    'tool_name': tools[2],
                    'tool_script_name': tools[3],
                    'created_on': tools[4]
                }
                tool_list.append(tool_json)
            tool_response = json.dumps(tool_list, cls=DateEncoder)
            print(tool_response)

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
