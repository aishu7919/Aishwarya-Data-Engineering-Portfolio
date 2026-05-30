from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generate_file_name(ti):

    file_name = "orders_2026_05_12.csv"

    ti.xcom_push(
        key='orders_file',
        value=file_name
    )

    print(f"Generated File Name: {file_name}")

def process_file(ti):

    received_file = ti.xcom_pull(
        key='orders_file',
        task_ids='generate_file_task'
    )

    print(f"Processing File: {received_file}")

with DAG(
    dag_id='xcom_dag',
    start_date=datetime(2026, 2, 14),
    schedule=None,
    catchup=False,
    tags=['xcom']
) as dag:

    task_1 = PythonOperator(
        task_id='generate_file_task',
        python_callable=generate_file_name
    )

    task_2 = PythonOperator(
        task_id='process_file_task',
        python_callable=process_file
    )

    task_1 >> task_2