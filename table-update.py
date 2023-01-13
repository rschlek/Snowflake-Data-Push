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
def push_snowflake(dataframe, desired_table_name, database, schema):
    # Connect to Snowflake
    con = snowflake.connector.connect(
        user=username,
        password=password,
        account=account
    )

    # Create a cursor
    cur = con.cursor()

    # Snowflake table
    table_name = desired_table_name
    table = '.'.join([database, schema, table_name])

    # check for unsupported data types
    unsupported_types = set(dataframe.dtypes.astype(str).unique()).difference(set(['int','float','bool','object','datetime64[ns]','float64','int64','datetime64[ns, UTC]']))
    if unsupported_types:
        raise ValueError(f"Unsupported datatype found: {unsupported_types}. Please make sure that all datatype can be mapped to a Snowflake data type.")
    # Check if table exists
    table_exists = cur.execute("SHOW TABLES LIKE '{}'".format(table))
    if not table_exists:
        # Create the table with the proper columns and data types
        columns = ''
        for col, dtype in zip(dataframe.columns, dataframe.dtypes):
            columns += col + ' ' + str(dtype) + ','
        columns = columns[:-1] # remove the last comma
        cur.execute("CREATE TABLE {}.{} ({})".format(schema, table_name, columns))
        con.commit()

    cur.execute("USE WAREHOUSE {}".format(warehouse))
    cur.execute("USE SCHEMA {}".format(schema))
    cur.execute("USE DATABASE {}".format(database))
    cur.execute("USE ROLE {}".format(role))

    # Insert data into the table
    data = [tuple(x) for x in dataframe.values]
    placeholders = ','.join(['%s'] * len(dataframe.columns))
    cur.executemany("INSERT INTO {} SELECT {}".format(table, placeholders), data)
    con.commit()

    # Close cursor and connection
    cur.close()
    con.close()