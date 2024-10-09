import export_data
import import_data

import configparser
import pandas as pd
from urllib.parse import urlparse
import snowflake.connector
import subprocess
import tempfile
import os
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from snowflake.connector.pandas_tools import write_pandas

# Read connection parameters from the properties file
config = configparser.ConfigParser()
config.read("SnowflakeConn.properties")
print("\n*********************")

# Parse the URL to extract the account name
source_url = config.get("Snowflake", "SOURCE_URL")
parsed_source_url = urlparse(source_url)
source_account = parsed_source_url.netloc.split('.')[0]
dest_url = config.get("Snowflake", "DEST_URL")
parsed_dest_url = urlparse(dest_url)
dest_account = parsed_dest_url.netloc.split('.')[0]

# function to remove the staged data
def remove_data_from_stage(conn, project_name, table):
    remove_query = f"REMOVE @~/{project_name}/{table};"
    with conn.cursor() as cursor:
        cursor.execute(remove_query)
    print(f"Data removed from stage: @~/{project_name}/{table}\n")

# function to get the list of tables
def get_table_list(conn, stage_name):
    list_query = f"LIST @{stage_name};"
    tables = []
    with conn.cursor() as cursor:
        cursor.execute(list_query)
        rows = cursor.fetchall()
        for row in rows:
            table_name = row[0].split('/')[-1].split('.')[0]  # Assuming table names are extractable from the path
            tables.append(table_name)
    return tables    

def main():
    # Set up Snowflake connections for source and destination
    source_conn = snowflake.connector.connect(
        user= config.get("Snowflake", "SOURCE_USER"),
        password= config.get("Snowflake", "SOURCE_PASSWORD"),
        account= source_account,
        warehouse= config.get("Snowflake", "SOURCE_WAREHOUSE"),
        database= config.get("Snowflake", "SOURCE_DB"),
        schema= config.get("Snowflake", "SOURCE_SCHEMA")
    )
    
    destination_conn = snowflake.connector.connect(
        user= config.get("Snowflake", "DEST_USER"),
        password= config.get("Snowflake", "DEST_PASSWORD"),
        account= dest_account,
        warehouse= config.get("Snowflake", "DEST_WAREHOUSE"),
        database= config.get("Snowflake", "DEST_DB"),
        schema= config.get("Snowflake", "DEST_SCHEMA")
    )
    
    project_name = "copy_table"

    try:
        # Create a temporary folder for storing data and DDL files
        tmp_folder = tempfile.mkdtemp(prefix='copy_table.')
        data_folder = os.path.join(tmp_folder, 'data')
        ddl_folder = os.path.join(tmp_folder, 'ddl')
        os.makedirs(data_folder)
        os.makedirs(ddl_folder)

        # Get the list of tables to copy from the stage
        table_list = get_table_list(source_conn, STAGE_NAME)
        
        # Loop through each table and perform the copy process
        for table in table_list:
            print(f"Processing table: {table}\n")

             # Fetch DDL for the source table
            ddl_file_path = os.path.join(ddl_folder, 'ddl.sql')
            export_data.fetch_ddl_for_table(source_conn, SOURCE_DB, SOURCE_SCHEMA, table, ddl_file_path)

            # Modify the DDL to fit the destination table
            import_data.modify_ddl_for_destination(ddl_file_path, table, DEST_DB, DEST_SCHEMA, table)

            # Export data from source
            export_data.copy_data_to_stage(source_conn, project_name, table, SOURCE_DB, SOURCE_SCHEMA)
            export_data.get_data_from_stage(source_conn, project_name, table, data_folder)

            # Apply the modified DDL in the destination
            with open(ddl_file_path, 'r') as ddl_file:
                ddl_query = ddl_file.read()
            with destination_conn.cursor() as cursor:
                cursor.execute(ddl_query)

            # Import data into destination
            import_data.put_data_to_stage(destination_conn, project_name, table, data_folder)
            import_data.copy_data_from_stage(destination_conn, project_name, table, DEST_DB, DEST_SCHEMA)
            
            #removing data from stages
            remove_data_from_stage(source_conn, project_name, table)
            remove_data_from_stage(destination_conn, project_name, table)
    
    finally:
        # Clean up and close connections
        source_conn.close()
        destination_conn.close()
        os.system(f"rm -rf {tmp_folder}")
        print("Temporary files cleaned up\n")

#INFO
STAGE_NAME = 'ZINGG_STAGE' # stage from where you want to copy data
SOURCE_DB = 'ZINGG'
SOURCE_SCHEMA = 'PUBLIC'
DEST_DB = 'ZINGG'
DEST_SCHEMA = 'PUBLIC'


if __name__ == "__main__":
    main()