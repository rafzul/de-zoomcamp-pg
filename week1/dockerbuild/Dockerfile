# base docker image
FROM python:3.9.1

#command that u want to run when base image set, in this case installing prequisites (Pandas) 
RUN pip install pandas sqlalchemy psycopg2

# set up working directory inside container where pipeline.py would be copied to
WORKDIR /app

# copy the script to the container. 1st name is source file in host machine, 2nd is destination in image (that then put inside the container). it can be the same
COPY ingest_data.py ingest_data.py

#defining what to do first when container runs
#here we simply run the script
ENTRYPOINT ["python", "ingest_data.py"]

