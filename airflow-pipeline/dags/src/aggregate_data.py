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

DATALAKE_MINIO_ENDPOINT = Variable.get('DATALAKE_MINIO_ENDPOINT')
DATALAKE_MINIO_ACCESS_KEY = Variable.get('DATALAKE_MINIO_ACCESS_KEY')
DATALAKE_MINIO_SECRET_KEY = Variable.get('DATALAKE_MINIO_SECRET_KEY')
DATALAKE_MINIO_CLEANED_BUCKET = Variable.get('DATALAKE_MINIO_CLEANED_BUCKET')
DATALAKE_MINIO_AGG_BUCKET = Variable.get('DATALAKE_MINIO_AGG_BUCKET')


def aggregate_data(**kwargs):
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

    try:
        # check if source bucket exist
        if dataLakeMinioClient.bucket_exists(DATALAKE_MINIO_CLEANED_BUCKET):
            logging.info(
                "[DATA-LAKE] Bucket `{}` is exist".format(DATALAKE_MINIO_CLEANED_BUCKET))
        else:
            raise ValueError('"[DATA-LAKE] Bucket is not exist"')

        # download CSV file
        source_object_name = "bank.xlsx_{}.csv".format(execution_date)
        destination = '{}/{}'.format(temp_dir_path, source_object_name)
        data = dataLakeMinioClient.get_object(
            DATALAKE_MINIO_CLEANED_BUCKET, source_object_name)
        with open(destination, 'wb') as file_data:
            for d in data.stream(32*1024):
                file_data.write(d)
            logging.info("[DATA-LAKE] Successfully downloaded file from `{}/{}` to `{}`".format(
                DATALAKE_MINIO_CLEANED_BUCKET, source_object_name, destination))

        # add to pandas dataframe
        df = pd.read_csv(destination)
        logging.info("Successfully read data from {}".format(destination))

        # transformation: group by 'Account No' and `VALUE DATE`, and aggregate with summarize
        df = df.groupby(['Account No', 'VALUE DATE'])[["WITHDRAWAL AMT", "DEPOSIT AMT"]].sum().reset_index()

        # logs transformation result
        logging.info("Transformation result: \n{}\n{}".format(df, df.dtypes))

        # store to data lake
        df.to_csv(destination)
        logging.info("Successfully save data to {}".format(destination))

        if dataLakeMinioClient.bucket_exists(DATALAKE_MINIO_AGG_BUCKET):
            logging.info("[DATA-LAKE] Bucket `{}` is exist".format(
                DATALAKE_MINIO_AGG_BUCKET))
        else:
            logging.info("[DATA-LAKE] Bucket `{}` is not exist, will attempt to create".format(
                DATALAKE_MINIO_AGG_BUCKET))
            dataLakeMinioClient.make_bucket(DATALAKE_MINIO_AGG_BUCKET)
            logging.info("[DATA-LAKE] Bucket `{}` is created".format(
                DATALAKE_MINIO_AGG_BUCKET))

        dataLakeMinioClient.fput_object(
            DATALAKE_MINIO_AGG_BUCKET, source_object_name, destination, content_type='application/csv')
        logging.info(
            "[DATA-LAKE] Successfully stored `{}`".format(source_object_name))

        # temp-data cleanup
        shutil.rmtree(temp_dir_path)
        logging.info("Delete directory `{}`".format(temp_dir_path))

    except InvalidBucketError as err:
        raise ValueError('"Error in bucket name"')
    except NoSuchKey as err:
        raise ValueError('"Object not found"')
    except ResponseError as err:
        raise ValueError('"Error In API call."')
