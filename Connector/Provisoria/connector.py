from hdbcli import dbapi
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
import pandas
import datetime

class framework:

    def sap_hana_connection(address, port, user, password):

        try:
            conn_sap = dbapi.connect(
                address = address,
                port = port,
                user = user,
                password = password)
            
            return conn_sap
            
        except Exception as e:
            print(f'error to connect SAP {address} with message {e}')
    


    def snowflake_connection(account, user, password, database, schema, warehouse, role):

        try:
            conn_snowflake = snowflake.connector.connect(
                account = account,
                user = user,
                password = password,
                database = database,
                schema = schema,
                warehouse = warehouse,
                role=role)
            
            query = datetime.datetime.now()
            print(f"Connected in Snowflake  {account} at {query}")
            return conn_snowflake

        except Exception as e:
            print(f'error to connect Snowflake account {account}  with message {e}')

        
    def snowflake_run_script(conn_snowflake, script):

        try:

            print(f'vou executar {script}')
            cs = conn_snowflake.cursor().execute(script)
            return cs
            
        except Exception as e:
            print(f'error to run query {script}   with message {e}')
            

    def pandas_run_query(conn_snowflake, script):

        try:

            print(f'i am going to run {script}')
            cs = pandas.read_sql_query(script,conn_snowflake)
            return cs

        except Exception as e:
            print(f'error to run query in pandas {script}   with message {e}')


    def pandas_write_snowflake(conn_snowflake, df, target_table):

        try:

            cs = write_pandas(conn_snowflake, df, target_table, auto_create_table=True, overwrite=True)
            return cs
        except Exception as e:
            print(f'error to write snowflake table using pandas {target_table}   with message {e}')


    if __name__ == '__main__':
         print('Generic connector called by itself' )
    else:
         print('Generic Connector called by another python program ' )
                                   