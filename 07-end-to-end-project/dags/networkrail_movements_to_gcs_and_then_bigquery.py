import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "deb2-17aug2023"
GCS_BUCKET = "deb2-bootcamp-200030"
BIGQUERY_DATASET = "networkrail"
KEYFILE_FOR_GCS = "deb2--loading-data-to-bigquery17aug2023-84dbb2a8efb6.json"
KEYFILE_FOR_GCS_TO_BIGQUERY = "deb2--loading-data-to-bigquery17aug2023-84dbb2a8efb6.json"


def _extract_data(**context):
    ds = context["data_interval_start"].to_date_string()

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
    # Connect to Postgres via Hook
    pg_hook = PostgresHook(
        postgres_conn_id="networkrail_postgres_conn",
        schema="networkrail"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    # Query movement data
    sql = f"""
        select * from movements where date(actual_timestamp) = '{ds}'
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    header = [
        "event_type",
        "gbtt_timestamp",
        "original_loc_stanox",
        "planned_timestamp",
        "timetable_variation",
        "original_loc_timestamp",
        "current_train_id",
        "delay_monitoring_point",
        "next_report_run_time",
        "reporting_stanox",
        "actual_timestamp",
        "correction_ind",
        "event_source",
        "train_file_address",
        "platform",
        "division_code",
        "train_terminated",
        "train_id",
        "offroute_ind",
        "variation_status",
        "train_service_code",
        "toc_id",
        "loc_stanox",
        "auto_expected",
        "direction_ind",
        "route",
        "planned_event_type",
        "next_report_stanox",
        "line_ind",
    ]
    with open(f"{DAGS_FOLDER}/movements-{ds}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    # Your code here
    if True:
        return "load_data_to_gcs"
    else:
        return "do_nothing"


def _load_data_to_gcs(**context):
    ds = context["data_interval_start"].to_date_string()

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
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_data_from_gcs_to_bigquery(**context):
    ds = context["data_interval_start"].to_date_string()

    # Your code here


default_args = {
    "owner": "Skooldio",
    "start_date": timezone.datetime(2023, 5, 1),
}
with DAG(
    dag_id="networkrail_movements_to_gcs_and_then_bigquery",
    default_args=default_args,
    schedule="@hourly",  # Set the schedule here
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Extract data from NetworkRail Postgres Database
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Do nothing
    do_nothing = EmptyOperator(task_id="do_nothing")

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")    

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = EmptyOperator(task_id="load_data_from_gcs_to_bigquery")










    # Task dependencies
    start >> extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end