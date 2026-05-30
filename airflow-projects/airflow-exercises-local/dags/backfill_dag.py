from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_orders(**kwargs):

    execution_date = kwargs['ds']

    file_name = f"orders_{execution_date}.csv"

    print(f"Processing historical file: {file_name}")

with DAG(
    dag_id='backfill_dag',
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=True,
    tags=['backfill']
) as dag:

    process_task = PythonOperator(
        task_id='process_orders_task',
        python_callable=process_orders
    )