import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

user = ''
password = ''
account = ''
warehouse = ''
database = ''
schema = ''

desired_table_name = ''

conn = snowflake.connector.connect(
    user = user,
    password = password,
    account =  account,
    warehouse = warehouse,
    database = database,
    schema = schema
)

cur = conn.cursor()

########## ADD CODE TO BUILD TABLE BELOW THIS COMMENT ##########

sql = ''
cur.execute(sql)
df = cur.fetch_pandas_all()








'''Function to push dataframe into snowflake, appends if table exists
@param dataframe is the pandas dataframe that will be appended to the snowflake table
@param snowflake_table_name is the name of the table in snowflake
'''
def push_snowflake(dataframe, snowflake_table_name):
    engine = create_engine(URL(
        account = account,
        user = user,
        password = password,
        database = database,
        schema = schema,
        warehouse = warehouse        
    ))
    
    connection = engine.connect()
    
    dataframe.to_sql(snowflake_table_name, con=engine,index=False,method=pd_writer,if_exists='append')
    
    connection.close()
    engine.dispose()