import snowflake.connector
import pandas as pd

# Declare the Snowflake connection details
username = ''
password = ''
account = ''
warehouse = ''
database = ''
schema = ''
role = ''

desired_table_name = ''























'''Function to push data to snowflake
if table name does not exist, table will be created
if table name exists in db, table will be appended
@param dataframe dataframe to push
@param desired_table_name is a string of the table name to be created or appended to in snowflake
@param database string of snowflake database to use
@param schema string of snowflake schema to use
'''
def push_snowflake(dataframe, table_name, username=username, password=password, database=database, schema=schema, role=role, warehouse=warehouse):
    
    # Connect to Snowflake
    con = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
    )
    cur = con.cursor()
    cur.execute("USE ROLE {}".format(role))
    cur.execute("USE DATABASE {}".format(database))
    cur.execute("USE SCHEMA {}".format(schema))

    # Check if table already exists
    cur.execute("SHOW TABLES LIKE '{}'".format(table_name))
    if not cur.fetchone():
        # Get column names and data types from dataframe
        columns = []
        for column in dataframe.columns:
            if dataframe[column].dtype == 'object':
                columns.append("{} VARCHAR(255)".format(column))
            elif dataframe[column].dtype == 'int64':
                columns.append("{} INTEGER".format(column))
            elif dataframe[column].dtype == 'float64':
                columns.append("{} FLOAT".format(column))
            elif dataframe[column].dtype == 'datetime64[ns]':
                columns.append("{} TIMESTAMP".format(column))
            else:
                raise ValueError("Data type {} not supported in Snowflake".format(dataframe[column].dtype))
        columns = ', '.join(columns)

        # Create table
        cur.execute("CREATE TABLE {}.{} ({})".format(schema, table_name, columns))
    
    # Get column names and placeholders for insert statement
    columns = ', '.join(dataframe.columns)
    placeholders = ', '.join(['%s' for _ in dataframe.columns])

    # Convert dataframe to list of tuples
    data = [tuple(x) for x in dataframe.to_numpy()]

    # Insert data into Snowflake table
    cur.executemany("INSERT INTO {}.{} ({}) VALUES ({})".format(schema, table_name, columns, placeholders), data)
    con.commit()
    cur.close()
    con.close()