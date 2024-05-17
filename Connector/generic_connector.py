from hdbcli import dbapi
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
import pandas 


class framework:

    def sap_hana_connection(address, port, user, password):
        
        try:
            conn_sap = dbapi.connect(
                address = address,
                port = port, 
                user = user, 
                password = password)

        except:
            print('error to connect SAP')   
        
        finally:
            print(f'connected in SAP Hana {address}')
            return conn_sap


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
            
        except:
            print('error to connect Snowflake') 

        finally:
            print(f'connected in Snowflake {account}')
            return conn_snowflake
        
    def snowflake_connection_sqlalchemy(account, user, password, database, schema, warehouse, role):

        try:

            engine = create_engine(
                    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
                    user=user,
                    password=password,
                    account=account,
                    database=database,
                    schema=schema,
                    warehouse=warehouse,
                    role=role,

                    ) )
            
        except:
            print('error to connect Snwoflake using SqlAlchemy ') 

        finally:
            print(f'connected in Snowflake {account} using SqlAlchemy ')
            return engine  

    def snowflake_run_script(conn_snowflake, script):

        try:

            print(f'vou executar {script}')
            cs = conn_snowflake.cursor().execute(script)

        except:
            print('error to run query') 

        finally:
            return cs
            #conn_snowflake.close()

    def pandas_run_query(conn_snowflake, script):

        try:

            print(f'i am going to run {script}')
            cs = pandas.read_sql_query(script,conn_snowflake)

        except:
            print('error to run query in pandas ') 

        finally:
            return cs
            #conn_snowflake.close()

    def sqlAlchemy_run_query(conn_snowflake, script):
        
        connection = conn_snowflake.connect()
        try:
            cs = connection.execute(script)
        finally:
            return cs 
            #connection.close()
            conn_snowflake.dispose()        


    def pandas_write_snowflake(conn_snowflake, df, target_table):

        try:
            cs = write_pandas(conn_snowflake=conn_snowflake, df=df, target_table=target_table, auto_create_table=True, overwrite=True)
        
        except:
            print('error to write snowflake table using pandas') 

        finally:
            print(f'written with success {target_table}') 
            #return cs
            #conn_snowflake.close()


#    def main():
#        snow = snowflake_run()
#        #test = sap_run()
#        #sap = connection_to_sap()

    if __name__ == '__main__':
         print('Generic connector called by itself' ) 
    else:
         print('Generic Connector called by another python program ' ) 
