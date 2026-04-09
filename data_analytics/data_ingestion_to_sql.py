import pandas as pd
import numpy as np
import os
import sqlalchemy as sa
import urllib
from dotenv import load_dotenv
import logging
import time
import warnings
warnings.filterwarnings('ignore')

if not os.path.exists('logs/'):
    os.makedirs('logs')

logging.basicConfig(filename='logs/data_ingestion_tosql.logs',
                    level=logging.DEBUG,
                    format="%(asctime)s-%(levelname)s-%(message)s",
                    filemode='a',
                    force=True)

#function to get primary keys from the files
def get_primary_keys(data_folder_name:str):
    try:
        data_files=[file for file in os.listdir(data_folder_name)]
        pk_map={}
        for file in data_files:
            if '.csv' in file:
                table_name=file[:-4]
                df_headers=pd.read_csv(f"{data_folder_name}/{file}",nrows=0)
                lower_headers=[col.lower() for col in df_headers.columns]
                expected_pk=f"{table_name.rstrip('s')}_id".lower()

                if expected_pk in lower_headers:
                    original_col_name=df_headers.columns[lower_headers.index(expected_pk)]
                    pk_map[table_name]=original_col_name
    except Exception as e:
        logging.error(e)
        raise
    return pk_map



def connection_engine_to_db():
    '''
    creates connection engine to master database and main database
    '''
    connection_start=time.time()
    logging.info('Started DB authentication and Connection')
    try:
        load_dotenv()
        # connecting to database
        server_name=os.getenv('db_server')
        user_name=os.getenv('db_username')
        password=os.getenv('db_password')
        new_db_name='marketing_e_commerce'

        # creating connection string to connect to master db
        master_db_connection_string=(
            f'DRIVER={{ODBC Driver 17 for SQL SERVER}};'
            f'SERVER={{{server_name}}};'
            f'DATABASE=master;'
            f'UID={{{user_name}}};'
            f'PWD={{{password}}};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes;'
        )

        #get params from the connection string
        master_db_params=urllib.parse.quote_plus(master_db_connection_string)

        # create master db engine
        master_db_engine=sa.create_engine(url=f'mssql+pyodbc:///?odbc_connect={master_db_params}',isolation_level='AUTOCOMMIT',fast_executemany=True)
    except Exception as e:
        logging.error(e)
        raise

    try:
        # converting master_db_connection_string to main_db_connection_string when the main db is present
        main_db_connection_string=master_db_connection_string.replace("DATABASE=master;",f"DATABASE={new_db_name};")

        main_db_params=urllib.parse.quote_plus(main_db_connection_string)

        main_db_engine=sa.create_engine(url=f'mssql+pyodbc:///?odbc_connect={main_db_params}',isolation_level='AUTOCOMMIT',fast_executemany=True)
    except Exception as e:
        logging.error(e)
        raise
    connection_end=time.time()
    total_connection_time=(connection_end-connection_start)/60
    logging.info(f"Total Connection time : {total_connection_time} minutes")

    return master_db_engine,main_db_engine




# function to update/insert data into existing tables and creating newer tables 
def upsert_to_db(df,table_name,engine,primary_key):
    upsert_start=time.time()
    transactional_tables=['transactions','master']
    static_tables=['campaigns','customers','events','products']
    
    # Creating Table if not in DB
    try:
        inspector=sa.inspect(engine)
        primary_key=primary_key
        if not inspector.has_table(table_name=table_name,schema='dbo'):
            logging.info(f'Table {table_name} Does not Exist Creating It....')
            df.head(0).to_sql(name=table_name,con=engine,schema='dbo',if_exists='replace',index=False)
            logging.info(f"Table {table_name} created successfully")
            
        #Creating staging table
        staging_table=f'stg_{table_name}'
        df.to_sql(name=staging_table,con=engine,if_exists='replace',schema='dbo',index=False,chunksize=10000)
    except Exception as e:
        logging.error(e)
        return

    try:
        #merging
        columns_to_update=[col for col in df.columns if col!=primary_key]
        update_strings=",".join([f"target.[{col}]=source.[{col}]" for col in columns_to_update])
        insert_cols=",".join(f"[{col}]" for col in df.columns)
        insert_values=",".join([f"source.[{col}]" for col in df.columns])
    except Exception as e:
        logging.error(e)
        return   
    upsert_query = f"""
            MERGE INTO dbo.[{table_name}] AS target
            USING dbo.[{staging_table}] AS source
            ON (target.[{primary_key}] = source.[{primary_key}])
            WHEN MATCHED THEN
                UPDATE SET {update_strings}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values});
            """

    append_query=f"""
    INSERT INTO dbo.[{table_name}] ({insert_cols})
    SELECT {insert_cols}
    FROM dbo.[{staging_table}];
    """

    try:
        if table_name in transactional_tables:
            with engine.begin() as conn:
                conn.execute(sa.text(append_query))
                conn.execute(sa.text(f"drop table dbo.{staging_table}"))
            logging.info(f"Append for {table_name} completed successfully.")
            
        else:
            with engine.begin() as conn:
                conn.execute(sa.text(upsert_query))
                conn.execute(sa.text(f"drop table dbo.{staging_table}"))
            logging.info(f"Upsert for {table_name} completed successfully.")
            
    except Exception as e:
        logging.error(e)
        return
    upsert_end=time.time()
    total_upsertion_time=(upsert_end-upsert_start)/60
    logging.info(f"Total Upsertion time : {total_upsertion_time} minutes")



#function to ingest streaming data
def stream_ingest_data(engine,path):
    ingestion_start=time.time()
    #create a primary_key map
    pk_map=get_primary_keys(path)

    try:
        data_files=[file for file in os.listdir(path)]
        if len(data_files)>0:
            for file in data_files:
                if '.csv' in file:
                    df=pd.read_csv(f'{path}/'+file,on_bad_lines='skip')
                    table_name=file[0:-4]
                    pk=pk_map.get(table_name,'id')
                    logging.info(f"Table {table_name} getting ingested")
                    upsert_to_db(df=df,table_name=table_name,engine=engine,primary_key=pk)
                    logging.info(f"Table {table_name} ingested Successfuly")
        else:
            logging.info(f"No files in stream_data folder")
            raise FileNotFoundError(f"no files available in stream_data folder")
    except Exception as e:
        logging.error(e)
        raise
    ingestion_end=time.time()
    total_ingestion_time=(ingestion_end-ingestion_start)/60
    logging.info(f"Total ingestion time taken : {total_ingestion_time} minutes")


# function to ingest initial data
def initial_ingest_data(engine,path):
    ingestion_start=time.time()
    #create a primary_key map
    pk_map=get_primary_keys(path)

    try:
        data_files=[file for file in os.listdir(path)]
        for file in data_files:
            if '.csv' in file:
                df=pd.read_csv(f'{path}/'+file,on_bad_lines='skip')
                table_name=file[0:-4]
                pk=pk_map.get(table_name,'id')
                logging.info(f"Table {table_name} getting ingested")
                df.to_sql(name=table_name,con=engine,schema='dbo',if_exists='replace',index=False,chunksize=10000)
                logging.info(f"Table {table_name} ingested Successfuly")
    except Exception as e:
        logging.error(e)
        raise
    ingestion_end=time.time()
    total_ingestion_time=(ingestion_end-ingestion_start)/60
    logging.info(f"Total ingestion time taken : {total_ingestion_time} minutes")    





if __name__=="__main__":
    db_name='marketing_e_commerce'
    master_engine,main_engine=connection_engine_to_db()
    
    initial_data_folder='initial_data'
    stream_data_folder='stream_data'
    
    with master_engine.connect() as conn:
        result=conn.execute(sa.text(f"select name from sys.databases;"))
        present_databases = [row[0] for row in result]
        if db_name in present_databases:
            stream_ingest_data(main_engine,stream_data_folder)
        else:
            #opening(with) and connecting to master db when the main db is not present
            with master_engine.connect() as conn:
                # if the databse exist then make ready for data ingestion else create and make ready for ingestion
                conn.execute(sa.text(f"if not exists (select * from sys.databases where name='{db_name}') create database {db_name};"))
                logging.info(f"Database {db_name} Created, Ready for Ingestion")
            initial_ingest_data(main_engine,initial_data_folder)


