#!/usr/bin/env python
# coding: utf-8

import os 
import argparse

from time import time 

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    #download the CSV
    os.system(f"wget {url} -O {csv_name}")
    #create sqlalchemy postgres engine `database://user:password@host:port/database_name`
    engine = create_engine(f'postgresql//{user}:{password}@{host}:{port}/{db}')
    #create iterator object to read csv as chunk
    deef_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    #get first chunk
    deef = next(deef_iter)
    #convert pickup time and dropoff time column to timestamps
    deef.tpep_pickup_datetime = pd.to_datetime(deef.tpep_pickup_datetime)
    deef.tpep_dropoff_datetime = pd.to_datetime(deef.tpep_dropoff_datetime)
    #finally, create table in the database (empty table first, generate only the column). schema is inferred from the fetched df
    deef.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    #get the iteration to get the first chunk of CSV and append to table
    deef.to_sql(name=table_name, con=engine, if_exists='append')

    #start the loop to get all the remaining chunks of downloaded csv
    while True:
        try:
            #get starting time 
            t_start = time()
            #get next chunks
            deef = next(deef_iter)
            #convert pickup time & dropoff time column type of next chunk
            deef.tpep_pickup_datetime = pd.to_datetime(deef.tpep_pickup_datetime)
            deef.tpep_dropoff_datetime = pd.to_do_datetime(deef.tpep_dropoff_datetime)
            #get the iteration to get next chunk of CSV and append to table 
            deef.to_sql(name=table_name, con=engine, if_exists='append')
            #get end time & print log messages
            t_end = time()
            print('inserted another chunk, took %.3f second' %(t_end - t_start))
        except StopIteration:
            print('completed')
            engine.dispose()
            break
if __name__ = __main__:
    #create the parser object
    parser = argparse.ArgumentParser(desription='process command line arguments')

    #adding arguments
    parser.add_argument('--user', help='username for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres where we will write the results of csv read loops')
    parser.add_arguments('--url', help='url of the csv file')

    #getting the arguments that we add in command line when we run the program by parsing all the inputted flag
    args = parser.parse_args()

    #running the main function with inputted args 
    main(args)
