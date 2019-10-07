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

        query = """SELECT 
                    max(package.package_id) as package_id, 
                    max(package.type) as type, 
                    max(package.description) as description, 
                    max(package.name) as name, 
                    max(package.doi) as doi, 
                    max(package.created_on) as created_on, 
                    max(package.created_by) as created_by, 
                    max(tool.tool_id) as tool_id, 
                    max(tool.description) as tool_description, 
                    max(tool.name) as tool_name, 
                    max(tool.script_name) as tool_script_name, 
                    array_agg(archive.name) as input_files 
                FROM package 
                    JOIN archive ON (package.archive_id = archive.archive_id)
                    JOIN tool ON (package.tool_id = tool.tool_id)
                GROUP BY 
                        package.package_id 
                ORDER BY {order_by} 
                LIMIT %s 
                OFFSET %s;""".format(order_by=actual_order_by)

        cursor.execute(query, (limit, offset))
        if cursor.rowcount == 0:
            return jsonify({"Error": "Query returns zero results."}), 404
        elif cursor.rowcount > 0:
            package_info = cursor.fetchall()
            package_list = []
            for packages in package_info:
                package_json = {
                    'package_id': packages[0],
                    'type': packages[1],
                    'description': packages[2],
                    'name': packages[3],
                    'doi': packages[4],
                    'created_on': packages[5],
                    'created_by': packages[6],
                    'tools': [{
                        'tool_id': packages[7], 
                        'description': packages[8], 
                        'name': packages[9], 
                        'created_by': None
                        # 'tool_script_name': packages[10]
                    }],
                    'input_files': packages[11]
                }
                package_list.append(package_json)
            package_response = json.dumps(package_list, cls=DateEncoder)
            print(package_response)

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
