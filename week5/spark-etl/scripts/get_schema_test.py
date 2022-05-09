import pandas as pd

df_pandas = pd.read_csv("/media/rafzul/Terminal Dogma/nytaxidata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}_head.csv)

spark.createDataFrame(df_pandas)