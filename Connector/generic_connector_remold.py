#from hdbcli import dbapi
import snowflake.connector
#from snowflake.sqlalchemy import URL
#from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd 
import datetime

class framework:

#   def sap_hana_connection(address, port, user, password):
#
#        try:
#            conn_sap = dbapi.connect(
#                address = address,
#                port = port,
#                user = user,
#                password = password)
#            
#            return conn_sap
#            
#        except:
#            print(f'error to connect SAP {address}')
    


    def snowflake_connection(account, user, password, database, schema, warehouse, role, script, target_table):

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
            print(' ')
            print(f'i am going to run {script}')
            #dfs = pd.DataFrame()  
            #dfl = [] 
            #dfs = pd.concat(pd.read_sql(script, conn_snowflake, chunksize=5000), ignore_index=True)   
            #cs = pandas.read_sql_query(script,conn_snowflake)
            #print(f'quantity of records table {target_table} ', dfs.count())
            #print(' ')
            #print(f'i am going to write target table {target_table}')
            #df = write_pandas(conn_snowflake, dfs, target_table, auto_create_table=True, overwrite=True)
            # Start Chunking
            #copied = 0 
            #qtd_records = 0 
            df_copy = ''
            for chunk in pd.read_sql(script, con=conn_snowflake, chunksize=1):
                #chunk.astype(str).apply(', '.join, axis=1)
                df_copy = chunk.iloc[:0,:].copy() 
                #df_copy = chunk.iloc[:0,:].copy()
                break
                #if  copied == 0:

            #df_copy = chunk.iloc[:0,:].copy()    
            write_pandas(conn_snowflake, df_copy, target_table, auto_create_table=True, overwrite=True)
            cs = conn_snowflake.cursor().execute(script)
            print(type(cs))
            #    break
                #    copied += 1
                # Start Appending Data Chunks from SQL Result set into List
                #qtd_records += 10000
                #print(qtd_records)
                #write_pandas(conn_snowflake, chunk, target_table, auto_create_table=False)
                #print(chunk.count())
            print(' ')
            print(f'table {target_table} written !')

            conn_snowflake.close()
            del conn_snowflake
            #return df

        except Exception as e:
            print(e)
            #print(f'error to connect Snowflake account {account}')

        

 #   def snowflake_run_script(conn_snowflake, script):
#
#        try:
#
#            print(f'vou executar {script}')
#            cs = conn_snowflake.cursor().execute(script)
#            return cs
#            
#        except:
#            print(f'error to run query {script}')
            

#    def pandas_run_query(conn_snowflake, script):
#
#        try:
#
#            print(f'i am going to run {script}')
#            cs = pandas.read_sql_query(script,conn_snowflake)
#            return cs
#
#        except:
#            print(f'error to run query in pandas {script}')



#    def pandas_write_snowflake(conn_snowflake, df, target_table):
#
#        try:
#
#            cs = write_pandas(conn_snowflake, df, target_table, auto_create_table=True, overwrite=True)
#            return cs
#        except:
#            print(f'error to write snowflake table using pandas {target_table}')


    if __name__ == '__main__':
         print('Generic connector called by itself' )
    else:
         print('Generic Connector called by another python program ' )
                                   