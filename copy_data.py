import export_data
import import_data
import snowflake.connector
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


# Common function to set up Snowflake connection
def setup_snowflake_connection(config, section):
    url = config.get(section, "URL")
    parsed_url = urlparse(url)
    account = parsed_url.netloc.split('.')[0]
    db = config.get(section, "DB")
    schema = config.get(section, "SCHEMA")
    
    conn = snowflake.connector.connect(
        user=config.get(section, "USER"),
        password=config.get(section, "PASSWORD"),
        account=account,
        warehouse=config.get(section, "WAREHOUSE"),
        database=db,
        schema=schema
    )
    
    return conn, db, schema

# function to remove the staged data
def remove_data_from_stage(conn, project_name, table):
    remove_query = f"REMOVE @~/{project_name}/{table};"
    with conn.cursor() as cursor:
        cursor.execute(remove_query)
    print(f"Data removed from stage: @~/{project_name}/{table}\n")

# function to get the list of tables
def get_table_list_stage(conn, stage_name):
    list_query = f"LIST @{stage_name};"
    tables = []
    with conn.cursor() as cursor:
        cursor.execute(list_query)
        rows = cursor.fetchall()
        for row in rows:
            table_name = row[0].split('/')[-1].split('.')[0]  # Assuming table names are extractable from the path
            tables.append(table_name)
    return tables   

def get_table_list_schema(conn, schema_name):
    list_query = f"SHOW TABLES IN SCHEMA {schema_name};"
    tables = []
    with conn.cursor() as cursor:
        cursor.execute(list_query)
        rows = cursor.fetchall()
        for row in rows:
            table_name = row[1]  # Assuming the second column holds the table name
            tables.append(table_name)
    return tables    

def main(source_conn, destination_conn, SOURCE_DB, SOURCE_SCHEMA, DEST_DB, DEST_SCHEMA,  project_name, datasource, tables_datasource):
    
    try:
        # Create a temporary folder for storing data and DDL files
        tmp_folder = tempfile.mkdtemp(prefix='copy_table.')
        data_folder = os.path.join(tmp_folder, 'data')
        ddl_folder = os.path.join(tmp_folder, 'ddl')
        os.makedirs(data_folder)
        os.makedirs(ddl_folder)

        # Get the list of tables to copy from the schema or stage
        if datasource.lower() == "schema":
            table_list = get_table_list_schema(source_conn, tables_datasource)
        else:
            table_list = get_table_list_stage(source_conn, tables_datasource)
        
        # Loop through each table and perform the copy process
        for table in table_list:
            print(f"Processing table: {table}\n")

             # Fetch DDL for the source table
            ddl_file_path = os.path.join(ddl_folder, 'ddl.sql')
            export_data.fetch_ddl_for_table(source_conn, SOURCE_DB, SOURCE_SCHEMA, table, ddl_file_path)

            # Modify the DDL to fit the destination table
            import_data.modify_ddl_for_destination(ddl_file_path, table, DEST_DB, DEST_SCHEMA, table)

            # Apply the modified DDL in the destination
            with open(ddl_file_path, 'r') as ddl_file:
                ddl_query = ddl_file.read()
            with destination_conn.cursor() as cursor:
                cursor.execute(ddl_query)

            # Export data from source
            export_data.copy_data_to_stage(source_conn, project_name, table, SOURCE_DB, SOURCE_SCHEMA)
            export_data.get_data_from_stage(source_conn, project_name, table, data_folder)

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

# Get user inputs
project_name = input("Enter the project name (to be used for staging and temporary storage): ")
datasource = input("Is the datasource a schema or a stage? Enter 'schema' or 'stage': ").strip().lower()
tables_datasource = input("Enter the source schema or stage name (e.g., abc.xyz or my_folder): ")

# Load configuration
config = configparser.ConfigParser()
config.read("SnowflakeConnSource.properties")
source_conn, SOURCE_DB, SOURCE_SCHEMA = setup_snowflake_connection(config, "Snowflake")
config.read("SnowflakeConnDest.properties")
destination_conn, DEST_DB, DEST_SCHEMA = setup_snowflake_connection(config, "Snowflake")

# Working
main(source_conn, destination_conn, SOURCE_DB, SOURCE_SCHEMA, DEST_DB, DEST_SCHEMA, project_name, datasource, tables_datasource)