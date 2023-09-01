from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator


class EnrichmentOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message
