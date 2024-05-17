from hdbcli import dbapi
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
import pandas


class framework:


    def source_connection(**parameters):

        if source_database == 'SAP_HANA':

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
            
        if source_database == 'SNOWFLAKE': 

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



    def snowflake_run_script(conn_snowflake, script):

        try:

            print(f'vou executar {script}')
            cs = conn_snowflake.cursor().execute(script)
                
        except:
            print('error to run query')

        finally:
            return cs
            conn_snowflake.close()

    def pandas_run_query(conn_snowflake, script):

        try:

            print(f'i am going to run {script}')
            cs = pandas.read_sql_query(script,conn_snowflake)

        except:
            print('error to run query in pandas ')

        finally:               
                return cs
#            conn_snowflake.close()


    def pandas_write_snowflake(conn_snowflake, df, target_table):

        try:

            cs = write_pandas(conn_snowflake, df, target_table, auto_create_table=True, overwrite=True)

        except:
            print('error to write snowflake table using pandas')

        finally:
            return cs
#            conn_snowflake.close()


    #def main(source_database, param1, param2, param3):
    def main(account, user ,password, database, schema, warehouse, role, query, target_table):
        conn_snowflake = framework.snowflake_connection(account, user, password, database, schema, warehouse, role)
        cs =  framework.pandas_run_query(conn_snowflake=conn_snowflake, script=query) 
        df_target = framework.pandas_write_snowflake(conn_snowflake, cs, target_table)
        return df_target
        conn_snowflake.close()


    if __name__ == '__main__':
         print('Generic connector called by itself' )
    else:
         print('Generic Connector called by another python program ' )
                                   