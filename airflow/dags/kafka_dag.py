from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 14),
    'schedule_interval': "@daily"
}

with DAG("snap_handler", default_args=default_args):
    """ # dag 1
    check_system >> [create_snap, update_snap]
    create_snap (tasK_group: trigger_execution >> take_snap) >> load_to_cloud
    update_snap (task_group: get_previous_snap >> trigger_new_generate >> enrich_snap) >> load_to_cloud

    # dag 2
    check_files >> [execute_journal, new_generation]
    execute_journal (task: fetch) >> enrich
    new_generation (task: generate) >> enrich

    
    """
