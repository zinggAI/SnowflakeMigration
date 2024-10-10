import configparser
import pandas as pd
from urllib.parse import urlparse
import snowflake.connector
import subprocess
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from snowflake.connector.pandas_tools import write_pandas

#********FUNCTIONS**********

# function to upload data files to stage
def put_data_to_stage(conn, project_name, destination_table, data_folder):
    put_query = f"PUT file://{data_folder}/*.csv.gz @~/{project_name}/{destination_table}"
    with conn.cursor() as cursor:
        cursor.execute(put_query)
    print(f"Data files uploaded from {data_folder} to stage: @~/{project_name}/{destination_table}\n")

# function to copy data from the stage to the Snowflake table
def copy_data_from_stage(conn, project_name, destination_table, destination_database, destination_schema):
    copy_query = f"""
    COPY INTO {destination_database}.{destination_schema}.{destination_table} 
    FROM @~/{project_name}/{destination_table}
    FILE_FORMAT = (
        type = 'csv', 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"', 
        skip_header = 1
    )
    PATTERN = '.*.csv.gz';
    """
    with conn.cursor() as cursor:
        cursor.execute(copy_query)
    print(f"Data copied from stage to table: {destination_database}.{destination_schema}.{destination_table}\n")

# function to modify the DDL for the destination table
def modify_ddl_for_destination(ddl_file_path, source_table, destination_database, destination_schema, destination_table):
    with open(ddl_file_path, 'r') as ddl_file:
        ddl_content = ddl_file.read()

    # Replace source table name with destination table name and schema
    modified_ddl = ddl_content.replace(source_table, f"{destination_database}.{destination_schema}.{destination_table}")

    with open(ddl_file_path, 'w') as ddl_file:
        ddl_file.write(modified_ddl)
    
    print(f"Modified DDL for {destination_table} saved to {ddl_file_path}\n")