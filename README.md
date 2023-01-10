# Snowflake-Data-Push
Template to create or update a snowflake table from a pandas data frame. Can be used on a cadence to monitor metrics over time.


[Helpful Guide](https://stephenallwright.com/write-to-snowflake-from-pandas-dataframe)


### Prerequisites

[Python3](https://www.python.org/downloads/)

[Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)

[SQLAlchemy](https://docs.sqlalchemy.org/en/14/intro.html#installation)

[Snowflake Connector SQL Alchemy Upgrade](https://docs.snowflake.com/en/user-guide/sqlalchemy.html)


To push the table to Snowflake, simply put the code that builds your desired table below the commented line, and then call the push_snowflake function below the function declaration ex: push_snowflake(df, 'this_is_a_sample_table_name')

Add appropriate credentials at top of script. The table will be added to the DB and Schema specified in the credentials. If you wish to place it somewhere other than the specified credentials, change the code directly in the push_snowflake function.
