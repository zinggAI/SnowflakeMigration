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

# function to copy data from Snowflake table to the stage
def copy_data_to_stage(conn, project_name, source_table, source_database, source_schema):
    copy_query = f"""
    COPY INTO @~/{project_name}/{source_table}
    FROM {source_database}.{source_schema}.{source_table}
    FILE_FORMAT = (
        type = 'CSV',
        field_delimiter = ',',
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"', 
        ESCAPE_UNENCLOSED_FIELD = '^',
        COMPRESSION = 'gzip',
        NULL_IF=()
    )
    HEADER=TRUE
    OVERWRITE=TRUE;
    """
    with conn.cursor() as cursor:
        cursor.execute(copy_query)
    print(f"Data copied to stage: @~/{project_name}/{source_table}\n")

# function to get the data from stage to a local folder
def get_data_from_stage(conn, project_name, source_table, data_folder):
    get_query = f"GET @~/{project_name}/{source_table} file://{data_folder}/"
    with conn.cursor() as cursor:
        cursor.execute(get_query)
    print(f"Data downloaded to folder: {data_folder}\n")

# function to fetch the DDL for the source table
def fetch_ddl_for_table(conn, source_database, source_schema, source_table, ddl_file_path):
    ddl_query = f"SELECT GET_DDL('TABLE', '{source_database}.{source_schema}.{source_table}');"
    with conn.cursor() as cursor:
        cursor.execute(ddl_query)
        ddl_result = cursor.fetchone()[0]  # Fetch the DDL result
    with open(ddl_file_path, 'w') as ddl_file:
        ddl_file.write(ddl_result)
    print(f"Fetched DDL for {source_table} and saved to {ddl_file_path}\n")    
