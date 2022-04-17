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
    #create iterator to read csv as chunk
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    deef = next(df_iter)
    #convert pickup time and dropoff time column to timestamps
    deef.tpep_pickup_datetime = pd.to_datetime(deef.tpep_pickup_datetime)
    deef.tpep_dropoff_datetime = pd.to_datetime(deef.tpep_dropoff_datetime)
    #get schema 
    tableschema = pd.io.sql.get_schema(deef,name=table_name)
    #finally, create table in the database




# The CSV file is very big and Pandas may not be able to handle it properly if the whole thing doesn't fit in RAM. We will only import 100 rows for now.
df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)
df
# We will now create the ***schema*** for the  database. The _schema_ is the structure of the database; in this case it describes the columns of our table. Pandas can output the SQL ***DDL*** (Data definition language) instructions necessary to create the schema.
# We need to provide a name for the table; we will use 'yellow_taxi_data'
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))
# Note that this only outputs the instructions, it hasn't actually created the table in the database yet.
# Convert `tpep_pickup_datetime` and `tpep_dropoff_datetime` from text to timestamps.
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))

# --------------------------basically yg di atas cuma latihan untuk mendapatkan schema dan ngubah tipe data
#connect to Postgres and create the table out of outputted DDL using sqlalchemy
# An ***engine*** specifies the database details in a URI. The structure of the URI is:
# `database://user:password@host:port/database_name`
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()
# we can now use our engine to get the specific output for Postgres
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
# We will now create an ***iterator*** that will allow us to read the CSV file in chunks and send them to the database. Otherwise, we may run into problems trying to send too much data at once.
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
df_iter
# Use the `next()` function to get the chunks using the iterator.
df = next(df_iter)
df
# This is a brand new dataframe, so we need to convert the time columns to timestamp format.
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df
# We will now finally create the table in the database. With `df.head(n=0)` we can get the name of the columns only, without any additional data. We will use it to generate a SQL instruction to generate the table.
#
# we need to provide the table name, the connection and what to do if the table already exists
# we choose to replace everything in case you had already created something by accident before.
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
# You can now use `pgcli -h localhost -p 5432 -u root -d ny_taxi` on a separate terminal to look at the database:
# 
#  `\dt` for looking at available tables.
#  `\d yellow_taxi_data` for describing the new table.

# Let's include our current chunk to our database and time how long it takes.
get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")

# Let's write a loop to write all chunks to the database. Use the terminal with `pgcli` to check the database after the code finishes running.
from time import time

while True: 
    try:
        t_start = time()
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    except StopIteration:
        print('completed')
        break
# And that's it! Feel free to go back to the [notes](../notes/1_intro.md#inserting-data-to-postgres-with-python)
