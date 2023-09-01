import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from google.cloud import storage

sys.path.append('/opt/airflow')  # used for importing module
import generation.data_generator



default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28),
    'schedule_interval': "None"
}

with DAG("customers", default_args=default_args):
    generate_journal = PythonOperator(task_id="execute_journal", python_callable=start)
    # retrieve_dataset = PythonOperator(task_id="get_enriched_dataset", python_callable=download_gcs_folder)

    # enrichment = SparkSubmitOperator()
    # load_to_cloud = LocalFilesystemToGCSOperator()

    # generate_journal >> retrieve_dataset >> enrichment >> load_to_cloud
