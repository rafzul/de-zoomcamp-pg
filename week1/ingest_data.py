#!/usr/bin/env python
# coding: utf-8

# This file was tested with MacOS using Conda for Python management.
# 
# Make sure that your Python env has `pandas` and `sqlalchemy` installed. I also had to install `psycopg2` manually.

# In[1]:


import pandas as pd
pd.__version__


# The CSV file is very big and Pandas may not be able to handle it properly if the whole thing doesn't fit in RAM. We will only import 100 rows for now.

# In[2]:


df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)
df


# We will now create the ***schema*** for the database. The _schema_ is the structure of the database; in this case it describes the columns of our table. Pandas can output the SQL ***DDL*** (Data definition language) instructions necessary to create the schema.

# In[3]:


# We need to provide a name for the table; we will use 'yellow_taxi_data'
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# Note that this only outputs the instructions, it hasn't actually created the table in the database yet.

# Note that `tpep_pickup_datetime` and `tpep_dropoff_datetime` are text fields even though they should be timestamps. Let's change that.

# In[4]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# Even though we have the DDL instructions, we still need specific instructions for Postgres to connect to it and create the table. We will use `sqlalchemy` for this.

# In[5]:


from sqlalchemy import create_engine


# An ***engine*** specifies the database details in a URI. The structure of the URI is:
# 
# `database://user:password@host:port/database_name`

# In[6]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[7]:


# run this cell when the Postgres Docker container is running
engine.connect()


# In[8]:


# we can now use our engine to get the specific output for Postgres
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# We will now create an ***iterator*** that will allow us to read the CSV file in chunks and send them to the database. Otherwise, we may run into problems trying to send too much data at once.

# In[9]:


df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
df_iter


# We can use the `next()` function to get the chunks using the iterator.

# In[10]:


df = next(df_iter)
df


# 
# This is a brand new dataframe, so we need to convert the time columns to timestamp format.

# In[11]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df


# We will now finally create the table in the database. With `df.head(n=0)` we can get the name of the columns only, without any additional data. We will use it to generate a SQL instruction to generate the table.

# In[12]:


# we need to provide the table name, the connection and what to do if the table already exists
# we choose to replace everything in case you had already created something by accident before.
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# You can now use `pgcli -h localhost -p 5432 -u root -d ny_taxi` on a separate terminal to look at the database:
# 
# * `\dt` for looking at available tables.
# * `\d yellow_taxi_data` for describing the new table.

# Let's include our current chunk to our database and time how long it takes.

# In[13]:


get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


# Back on the terminal running `pgcli`, we can check how many lines were to the database with:
# 
# ```sql
# SELECT count(1) FROM yellow_taxi_data;
# ```
# 
# You should see 100,000 lines.
# 

# Let's write a loop to write all chunks to the database. Use the terminal with `pgcli` to check the database after the code finishes running.

# In[ ]:


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
