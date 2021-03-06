from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.extract_data import extract_data
from src.clean_data import clean_data
from src.aggregate_data import aggregate_data
from src.load_data import load_data
import requests
import json
import os


# Define the default dag arguments.
default_args = {
    'owner': 'Aditya Satrya',
    'depends_on_past': False,
    'email': ['aditya.satrya@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


# Define the dag, the start date and how frequently it runs.
# The dag to run every 2 hours, starting at 02:00 everyday
dag = DAG(
    dag_id='transactionDag',
    default_args=default_args,
    start_date=datetime(2015, 1, 1, 0, 0),
    schedule_interval=timedelta(hours=2))


# First task is to download XLSX file and extract its data
task1 = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag)


# Second task is to clean the data
task2 = PythonOperator(
    task_id='clean_data',
    provide_context=True,
    python_callable=clean_data,
    dag=dag)

# Third task is to aggregate data
task3 = PythonOperator(
    task_id='aggregate_data',
    provide_context=True,
    python_callable=aggregate_data,
    dag=dag)

# Third task is to load data into the database.
task4 = PythonOperator(
    task_id='load_data',
    provide_context=True,
    python_callable=load_data,
    dag=dag)

# set dependency
task1 >> task2 >> task3 >> task4
