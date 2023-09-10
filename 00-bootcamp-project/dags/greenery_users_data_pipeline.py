import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "dataengineercafe"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "users"


def _extract_data(ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "user_id",
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "create_at",
                "update_at",
                "address",
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["user_id"],
                    each["first_name"],
                    each["last_name"],
                    each["emai"],
                    each["phone_number"],
                    each["create_at"],
                    each["update_at"],
                    each["address"]
                ]
                writer.writerow(data)


def _load_data_to_gcs(ds):
    # YOUR CODE HERE
    keyfile_gcs = f"{DAGS_FOLDER}/deb2-loading-file-to-gcs-17216c60e1f1.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb2-bootcamp-200030"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _load_data_from_gcs_to_bigquery(ds):
    # YOUR CODE HERE
    keyfile_bigquery = f"{DAGS_FOLDER}/deb2-loading-data-to-gcs-then-bigquey-17aug2023-950eb2fe7488.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{PROJECT_ID}.deb_bootcamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = "deb20-bootcamp-200030"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_users_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = EmptyOperator(
        task_id="extract_data",
    )

    # Load data to GCS
    load_data_to_gcs = EmptyOperator(
        task_id="load_data_to_gcs",
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = EmptyOperator(
        task_id="load_data_from_gcs_to_bigquery",
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery