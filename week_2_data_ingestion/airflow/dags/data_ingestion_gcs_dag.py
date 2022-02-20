import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'

REQUIRED_DATA = [
    {
        'type': 'yellow',
        'url': 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv',
        'dataset_file': 'yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}',
        'start_date': datetime(year=2019, month=1, day=1),
        'frequency': '@monthly',
    },
    {
        'type': 'green',
        'url': 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv',
        'dataset_file': 'green_tripdata_{{ execution_date.strftime("%Y-%m") }}',
        'start_date': datetime(year=2019, month=1, day=1),
        'frequency': '@monthly',
    },
    {
        'type': 'fhv',
        'url': 'https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv',
        'dataset_file': 'fhv_tripdata_{{ execution_date.strftime("%Y-%m") }}',
        'start_date': datetime(year=2019, month=1, day=1),
        'frequency': '@monthly',
    },
    {
        'type': 'zone_lookup',
        'url': 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv',
        'dataset_file': 'taxi_zone_lookup',
        'start_date': datetime(year=2019, month=1, day=1),
        'frequency': '@once',
    }
]

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    return f"gs://{BUCKET}/{object_name}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def parameterized_dag_definition(
    type,
    frequency,
    start_date,
    url,
    dataset_file,
):
    with DAG(
        dag_id=f"data_ingestion_gcs_dag_{type}",
        schedule_interval=f"{frequency}",
        default_args=default_args,
        start_date=start_date,
        max_active_runs=1,
        tags=['dtc-de'],
    ) as dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -SL { url } > {path_to_local_home}/dags/{dataset_file}.csv"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/dags/{dataset_file}.csv",
            },
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{type}/{dataset_file}.parquet",
                "local_file": f"{path_to_local_home}/dags/{dataset_file}.parquet",
            },
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{type}_external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{type}/*.parquet"],
                },
            },
        )

        remove_local_downloaded_files = BashOperator(
            task_id="remove_local_downloaded_files",
            bash_command=f"rm '{path_to_local_home}/dags/{dataset_file}.parquet' '{path_to_local_home}/dags/{dataset_file}.csv'"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> remove_local_downloaded_files
        return dag

# NOTE: DAG declaration - using a Context Manager (an implicit way)
dags = [None for _ in range(len(REQUIRED_DATA))]
for idx, dag_definition in enumerate(REQUIRED_DATA):
    dags[idx] = parameterized_dag_definition(**dag_definition)
dag1, dag2, dag3, dag4 = dags
