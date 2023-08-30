from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

