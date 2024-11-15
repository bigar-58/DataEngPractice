FROM python:3.11

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

#Sets the working directory for the docker image and copies the python script
WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]