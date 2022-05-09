#!/bin/bash

LOCAL_PREFIX="/media/rafzul/Terminal Dogma/nytaxidata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"


!head -n 1001 ${LOCAL_PATH} > ${LOCAL_PATH}_head.csv