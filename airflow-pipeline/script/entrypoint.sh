#!/usr/bin/env bash

# wait database server ready
printf "Waiting for database...\n\n"
sleep 10

# install custom DAGs' dependency
printf "Installing custom DAGs' dependencies...\n\n"
pip install -r dags/requirements.txt

# initialize the database
printf "Running initdb...\n\n"
airflow initdb

# set airflow variables
printf "Setting airflow variables...\n\n"
airflow variables --set SOURCE_MINIO_ENDPOINT $SOURCE_MINIO_ENDPOINT
airflow variables --set SOURCE_MINIO_ACCESS_KEY $SOURCE_MINIO_ACCESS_KEY
airflow variables --set SOURCE_MINIO_SECRET_KEY $SOURCE_MINIO_SECRET_KEY
airflow variables --set SOURCE_MINIO_BUCKET $SOURCE_MINIO_BUCKET

airflow variables --set DATALAKE_MINIO_ENDPOINT $DATALAKE_MINIO_ENDPOINT
airflow variables --set DATALAKE_MINIO_ACCESS_KEY $DATALAKE_MINIO_ACCESS_KEY
airflow variables --set DATALAKE_MINIO_SECRET_KEY $DATALAKE_MINIO_SECRET_KEY
airflow variables --set DATALAKE_MINIO_RAW_BUCKET $DATALAKE_MINIO_RAW_BUCKET

# start the scheduler and webserver
printf "Running scheduler and webserver...\n\n"
/usr/bin/supervisord