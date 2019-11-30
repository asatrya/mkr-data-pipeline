#!/usr/bin/env bash

# wait database server ready
printf "Waiting for database...\n\n"
sleep 10

# initialize the database
printf "Running initdb...\n\n"
airflow initdb

# start the scheduler and webserver
printf "Running scheduler and webserver...\n\n"
/usr/bin/supervisord