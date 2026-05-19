from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def airflow_context(**kwargs):

    print("Airflow Runtime Context")

    print(f"Execution Date: {kwargs['ds']}")
    print(f"DAG ID: {kwargs['dag'].dag_id}")
    print(f"Task ID: {kwargs['task'].task_id}")
    print(f"Run ID: {kwargs['run_id']}")

with DAG(
    dag_id="kwargs_airflow_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["kwargs"]
) as dag:

    context_task = PythonOperator(
        task_id="context_task",
        python_callable=airflow_context
    )