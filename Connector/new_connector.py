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
            
        except:
            print(f'error to connect SAP {address}')
    


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

        except:
            print(f'error to connect Snowflake account {account}')

        
    def snowflake_run_script(conn_snowflake, script):

        try:

            print(f'vou executar {script}')
            cs = conn_snowflake.cursor().execute(script)
            return cs
            
        except:
            print(f'error to run query {script}')
            

    def pandas_run_query(conn_snowflake, script):

        try:

            print(f'i am going to run {script}')
            cs = pandas.read_sql_query(script,conn_snowflake)
            return cs

        except:
            print(f'error to run query in pandas {script}')


    def pandas_write_snowflake(cls, conn_snowflake, df, target_table):

        try:

            cs = write_pandas(cls.conn_snowflake, df, target_table, auto_create_table=True, overwrite=True)
            return cs
        except:
            print(f'error to write snowflake table using pandas {target_table}')


    if __name__ == '__main__':
         print('Generic connector called by itself' )
    else:
         print('Generic Connector called by another python program ' )
                                   