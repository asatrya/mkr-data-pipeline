from airflow.models import Variable

from minio import Minio
from minio.error import ResponseError
from minio.error import InvalidBucketError
from minio.error import NoSuchKey

import os
import shutil
import pandas as pd
import logging

SOURCE_MINIO_ENDPOINT = Variable.get('SOURCE_MINIO_ENDPOINT')
SOURCE_MINIO_ACCESS_KEY = Variable.get('SOURCE_MINIO_ACCESS_KEY')
SOURCE_MINIO_SECRET_KEY = Variable.get('SOURCE_MINIO_SECRET_KEY')
SOURCE_MINIO_BUCKET = Variable.get('SOURCE_MINIO_BUCKET')

DATALAKE_MINIO_ENDPOINT = Variable.get('DATALAKE_MINIO_ENDPOINT')
DATALAKE_MINIO_ACCESS_KEY = Variable.get('DATALAKE_MINIO_ACCESS_KEY')
DATALAKE_MINIO_SECRET_KEY = Variable.get('DATALAKE_MINIO_SECRET_KEY')
DATALAKE_MINIO_RAW_BUCKET = Variable.get('DATALAKE_MINIO_RAW_BUCKET')


def extract_data(**kwargs):
    """
    Download XLSX file from data source bucket 
    and store it on data lake bucket in CSV format
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

    # connect to source object storage
    sourceMinioClient = Minio(SOURCE_MINIO_ENDPOINT,
                              access_key=SOURCE_MINIO_ACCESS_KEY,
                              secret_key=SOURCE_MINIO_SECRET_KEY,
                              secure=False)

    # connect to data lake object storage
    dataLakeMinioClient = Minio(DATALAKE_MINIO_ENDPOINT,
                                access_key=DATALAKE_MINIO_ACCESS_KEY,
                                secret_key=DATALAKE_MINIO_SECRET_KEY,
                                secure=False)

    try:
        # check if source bucket exist
        if sourceMinioClient.bucket_exists(SOURCE_MINIO_BUCKET):
            logging.info("[DATA-SOURCE] Bucket `{}` is exist".format(SOURCE_MINIO_BUCKET))
        else:
            raise ValueError('"[DATA-SOURCE] Bucket is not exist"')

        # download XLSX file
        file_name = 'bank.xlsx'
        destination = '{}/{}'.format(temp_dir_path, file_name)
        data = sourceMinioClient.get_object(SOURCE_MINIO_BUCKET, file_name)
        with open(destination, 'wb') as file_data:
            for d in data.stream(32*1024):
                file_data.write(d)
            logging.info("[DATA-SOURCE] Successfully downloaded file from `{}/{}` to `{}`".format(
                SOURCE_MINIO_BUCKET, file_name, destination))

        # add to pandas dataframe
        df = pd.read_excel(destination, sheet_name='Sheet1')
        logging.info("Successfully read data from {}".format(destination))
        df.to_csv(destination + ".csv")
        logging.info("Successfully save data to {}".format(
            destination + ".csv"))

        # store to data lake
        if dataLakeMinioClient.bucket_exists(DATALAKE_MINIO_RAW_BUCKET):
            logging.info("[DATA-LAKE] Bucket `{}` is exist".format(
                DATALAKE_MINIO_RAW_BUCKET))
        else:
            logging.info("[DATA-LAKE] Bucket `{}` is not exist, will attempt to create".format(
                DATALAKE_MINIO_RAW_BUCKET))
            dataLakeMinioClient.make_bucket(DATALAKE_MINIO_RAW_BUCKET)
            logging.info("[DATA-LAKE] Bucket `{}` is created".format(
                DATALAKE_MINIO_RAW_BUCKET))

        object_name = "{}_{}.csv".format(file_name, execution_date)
        dataLakeMinioClient.fput_object(
            DATALAKE_MINIO_RAW_BUCKET, object_name, destination + '.csv', content_type='application/csv')
        logging.info("[DATA-LAKE] Successfully stored `{}`".format(object_name))

        # temp-data cleanup
        shutil.rmtree(temp_dir_path)
        logging.info("Delete directory `{}`".format(temp_dir_path))

    except InvalidBucketError as err:
        raise ValueError('"Error in bucket name"')
    except NoSuchKey as err:
        raise ValueError('"Object not found"')
    except ResponseError as err:
        raise ValueError('"Error In API call."')
