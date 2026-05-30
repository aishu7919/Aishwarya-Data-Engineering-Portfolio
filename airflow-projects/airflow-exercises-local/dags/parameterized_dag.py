from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_orders(**kwargs):

    execution_date = kwargs['ds']

    file_name = f"orders_{execution_date}.csv"

    print(f"Processing file: {file_name}")

with DAG(
    dag_id="parameterized_dag",
    start_date=datetime(2026, 2, 14),
    schedule="@daily",
    catchup=False,
    tags=["parameterized"]
) as dag:

    process_task = PythonOperator(
        task_id="process_orders_task",
        python_callable=process_orders
    )