#!/bin/bash
#setting up bash script for downloading data (minus the looping, will be done together with the parquetization task for each taxi type/month/   x`year inside dag)

set -e 

FMONTH= `printf "%02d" ${MONTH}`
URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PREFIX="/media/rafzul/Terminal Dogma/nytaxidata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

echo "downloading ${URL} to ${LOCAL_PATH}"
mkdir -p ${LOCAL_PREFIX}
wget ${URL} -O ${LOCAL_PATH}

echo "compressing ${LOCAL_PATH}"
gzip