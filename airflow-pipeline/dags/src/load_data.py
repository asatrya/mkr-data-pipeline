from airflow.models import Variable

from minio import Minio
from minio.error import ResponseError
from minio.error import InvalidBucketError
from minio.error import NoSuchKey

import os
import shutil
import pandas as pd
import logging
import datetime
from airflow.hooks.mysql_hook import MySqlHook
import sqlalchemy as db

DATALAKE_MINIO_ENDPOINT = Variable.get('DATALAKE_MINIO_ENDPOINT')
DATALAKE_MINIO_ACCESS_KEY = Variable.get('DATALAKE_MINIO_ACCESS_KEY')
DATALAKE_MINIO_SECRET_KEY = Variable.get('DATALAKE_MINIO_SECRET_KEY')
DATALAKE_MINIO_AGG_BUCKET = Variable.get('DATALAKE_MINIO_AGG_BUCKET')


def extract_latest_date(db_engine, table, field):
    """
    Function to extract latest date from a table
    """
    query = "SELECT MAX({}) AS max_date FROM {}".format(field, table)
    result_df = pd.read_sql(query, db_engine)
    return result_df.loc[0, 'max_date']


def is_weekend(row):
    """
    Function to determine whether a date row is weekend or not
    """
    if row['dayofweek'] == 5 or row['dayofweek'] == 6:
        return 1
    else:
        return 0


def create_date_table(start, end):
    """
    Function to generate date in particular range
    """
    df = pd.DataFrame({"date": pd.date_range(start, end)})
    df["dayofweek"] = df.date.dt.dayofweek
    df["day"] = df.date.dt.day
    df["week"] = df.date.dt.weekofyear
    df["month"] = df.date.dt.month
    df["quarter"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    df["is_weekend"] = df.apply(lambda row: is_weekend(row), axis=1)
    df["is_holiday"] = df.apply(lambda row: is_weekend(row), axis=1)
    df["date_key"] = df.date.dt.strftime('%Y%m%d')
    return df[['date_key', 'day', 'date', 'year', 'quarter', 'month', 'week', 'is_weekend', 'is_holiday']]


def get_account_id(x, account_df):
    """
    Get account id from account number
    """
    return account_df.loc[account_df['account_number'] == str(x)].iloc[0]['id']


def load_data(**kwargs):
    """
    Aggregate data
    """

    # logs information
    execution_date = kwargs["execution_date"]
    logging.info("Execution datetime={}".format(execution_date))

    # temp data directory
    temp_dir_path = os.path.join(os.path.dirname(__file__),
                                 '..', '..',
                                 'temp-data',
                                 kwargs["dag"].dag_id,
                                 kwargs["task"].task_id,
                                 str(execution_date))
    if not os.path.exists(temp_dir_path):
        os.makedirs(temp_dir_path)
    logging.info("Will write output to {}".format(temp_dir_path))

    # connect to data lake object storage
    dataLakeMinioClient = Minio(DATALAKE_MINIO_ENDPOINT,
                                access_key=DATALAKE_MINIO_ACCESS_KEY,
                                secret_key=DATALAKE_MINIO_SECRET_KEY,
                                secure=False)

    # Connection URI of OLAP database
    connection_uri_olap = "mysql+pymysql://dw:dw@data_warehouse:3306/transaction_dw"
    olap_db_engine = db.create_engine(connection_uri_olap)

    try:
        # check if source bucket exist
        if dataLakeMinioClient.bucket_exists(DATALAKE_MINIO_AGG_BUCKET):
            logging.info(
                "[DATA-LAKE] Bucket `{}` is exist".format(DATALAKE_MINIO_AGG_BUCKET))
        else:
            raise ValueError('"[DATA-LAKE] Bucket is not exist"')

        # download CSV file
        source_object_name = "bank.xlsx_{}.csv".format(execution_date)
        destination = '{}/{}'.format(temp_dir_path, source_object_name)
        data = dataLakeMinioClient.get_object(
            DATALAKE_MINIO_AGG_BUCKET, source_object_name)
        with open(destination, 'wb') as file_data:
            for d in data.stream(32*1024):
                file_data.write(d)
            logging.info("[DATA-LAKE] Successfully downloaded file from `{}/{}` to `{}`".format(
                DATALAKE_MINIO_AGG_BUCKET, source_object_name, destination))

        # add to pandas dataframe
        df = pd.read_csv(destination)
        logging.info(
            "Successfully read data from {}. Table:\n{}".format(destination, df))

        ##########################################
        # LOAD DATE DIMENSION
        ##########################################

        # get smallest date value from data
        min_date = pd.to_datetime(df['VALUE DATE'].min())
        max_date = pd.to_datetime(df['VALUE DATE'].max())

        # extract latest value from date dimension
        dateDim_latest = extract_latest_date(olap_db_engine, 'dimDate', 'date')
        if dateDim_latest != None:
            min_date = dateDim_latest + pd.Timedelta(1, unit='D')

        # load generated date range to date dimension table
        if max_date > min_date:
            date_range_df = create_date_table(min_date, max_date)
            logging.info("Generated date range from {} to {}. Generated table: \n{}".format(
                min_date, max_date, date_range_df))

            date_range_df.to_sql('dimDate', olap_db_engine,
                                 if_exists='append', index=False)
            logging.info(
                "Successfully inserted date range to date dimension table in data warehouse")
        else:
            logging.info(
                "There's no need to insert any data to date dimension table in data warehouse")

        ##########################################
        # LOAD ACCOUNT DIMENSION
        ##########################################

        # get unique account number in data
        account_numbers = df['Account No'].unique()

        # get account numbers in account number dimension (data warehouse)
        query = "SELECT id, account_number FROM accountNumberDim"
        account_df = pd.read_sql(query, olap_db_engine)

        # insert new account to account dimension table
        new_account_df = pd.DataFrame()
        for account_number in account_numbers:
            if str(account_number) not in account_df.values:
                logging.info(
                    "Account number `{}` is not yet in dimension table".format(account_number))
                new_account_df = new_account_df.append(
                    {'account_number': str(account_number)}, ignore_index=True)
        logging.info("Will insert this table to accountNumberDim dimension table:\n{}".format(
            new_account_df))
        new_account_df.to_sql('accountNumberDim',
                              olap_db_engine, if_exists='append', index=False)
        logging.info("Successfully inserted new account numbers")

        ##########################################
        # LOAD TRANSACTION (TRX) DATA
        ##########################################

        # get account numbers in account number dimension (data warehouse)
        query = "SELECT id, account_number FROM accountNumberDim"
        account_df = pd.read_sql(query, olap_db_engine)

        # prepare table to insert to database
        trx_df = df[['Account No', 'VALUE DATE',
                     'WITHDRAWAL AMT', 'DEPOSIT AMT']]
        trx_df.columns = ['account_number', 'date',
                          'withdrawal_amount', 'deposit_amount']
        trx_df['date'] = trx_df['date'].map(
            lambda x: pd.to_datetime(x).strftime('%Y%m%d'))
        trx_df['account_number_id'] = trx_df['account_number'].map(
            lambda x: get_account_id(x, account_df))
        trx_df = trx_df.drop(['account_number'], axis=1)
        logging.info("Will insert this table to trxAmountFact table:\n{}".format(trx_df))
        trx_df.to_sql('trxAmountFact',
                              olap_db_engine, if_exists='append', index=False)
        logging.info("Successfully inserted new transaction data")

        # temp-data cleanup
        shutil.rmtree(temp_dir_path)
        logging.info("Delete directory `{}`".format(temp_dir_path))

    except InvalidBucketError as err:
        raise ValueError('"Error in bucket name"')
    except NoSuchKey as err:
        raise ValueError('"Object not found"')
    except ResponseError as err:
        raise ValueError('"Error In API call."')
