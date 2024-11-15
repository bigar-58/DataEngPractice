from sqlalchemy import create_engine
import pandas as pd
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tableName = params.table_name
    url = params.url

    #CSV Destination file
    csv_name = 'output.csv'

    #Download csv file and output results to the csv_name file
    os.system(f"wget {url} -O {csv_name}")
    
    #Create postgres engine for connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #CSV Pre-Processing/Set-up
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #Create a table for yellow taxi data
    df.head(n=0).to_sql(name=tableName, con=engine, if_exists='replace')

    #Add first batch of data to the table 
    df.to_sql(name=tableName , con=engine, if_exists='append')

    #Iterate through batches of data in Taxi dataset and upload to postgres instance
    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=tableName, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == "__main__":
    #Create argument parser
    parser = argparse.ArgumentParser(description='Injest CSV Data to Postgres')

    #Configure script inputs: user, password host, port, database name, table name, url of csv
    parser.add_argument('--user', help='User name for postgres')
    parser.add_argument('--password', help='Password for postgres connection')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--db', help='Database name for postgres')
    parser.add_argument('--table_name', help='Table name for postgres')
    parser.add_argument('--url', help='Url of csv file to injest to postgres')

    args = parser.parse_args()
    
    main(args)
