import os
import pyarrow.parquet as pq
import pyarrow.csv as pv
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from astro import sql as aql
from  astro.files import File
from  astro.sql.table import Table, Metadata
from  astro.constants import FileType




data_folder = "/usr/local/airflow/data"

# format csv to parquet
def format_to_parquet(src_dir):
    for file_name in os.listdir(src_dir):
        if file_name.endswith('.csv'):
            csv_file_path = os.path.join(src_dir, file_name)
            table = pv.read_csv(csv_file_path)
            pq.write_table(table, csv_file_path.replace('.csv', '.parquet'))

# upload parquet files to google cloud storage
def upload_to_gcs(data_folder, gcs_path,**kwargs):
    data_folder = data_folder
    bucket_name = 'nyc-bike-share-2025-bucket'  # Your GCS bucket name
    gcs_conn_id = 'gcp'
    # List all parquet files in the data folder
    parquet_files = [file for file in os.listdir(data_folder) if file.endswith('.parquet')]

    # Upload each CSV file to GCS
    for parquet_file in parquet_files:
        local_file_path = os.path.join(data_folder, parquet_file)
        gcs_file_path = f"{gcs_path}/{parquet_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)


with DAG(
    "download_and_unzip_from_urls",
    start_date=datetime(2025, 4, 6),
    schedule_interval=None,
    catchup=False,
) as dag:
    download_and_unzip = BashOperator(
        task_id="download_and_unzip",
        bash_command=""" mkdir -p /usr/local/airflow/data && echo https://s3.amazonaws.com/tripdata/JC-2025{01..02}-citibike-tripdata.csv.zip | xargs -n 1 -P 2 wget -P /usr/local/airflow/data/ && unzip '/usr/local/airflow/data/*.zip' -d /usr/local/airflow/data"""
    )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs= {"src_dir": f"{data_folder}"}
    )
    remove_zip_and_csv_file = BashOperator(
        task_id ="remove_zip_and_csv_file",
        bash_command = "rm /usr/local/airflow/data/*.zip /usr/local/airflow/data/*.csv"
    )
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=['/usr/local/airflow/data', 'raw'],
        provide_context=True
    )
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        source_objects=["*.parquet"],
        source_format="PARQUET",
        skip_leading_rows=1,
        destination_project_dataset_table="NYC_BIKE_SHARE_dataset.NYC_BIKE_SHARE_data",
        write_disposition="WRITE_TRUNCATE",
        # project_id=PROJECT_ID,
        bucket="nyc-bike-share-2025-bucket",
        # location=GOOGLE_CLOUD_LOCATION,
        gcp_conn_id='gcp',
    )
    
    download_and_unzip >> format_to_parquet_task >> remove_zip_and_csv_file >> upload_to_gcs >> load_gcs_to_bigquery