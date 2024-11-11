FROM python:3.11

RUN pip install pandas
RUN pip install pyarrow

#Sets the working directory for the docker image and copies the python script
WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]