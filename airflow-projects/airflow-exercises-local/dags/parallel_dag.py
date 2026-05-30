from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_task():
    print("Pipeline Started")

def parallel_task_1():
    print("Running Parallel Task 1")

def parallel_task_2():
    print("Running Parallel Task 2")

def end_task():
    print("Pipeline Finished")

with DAG(
    dag_id="parallel_airflow_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_task
    )

    task_2 = PythonOperator(
        task_id="parallel_task_1",
        python_callable=parallel_task_1
    )

    task_3 = PythonOperator(
        task_id="parallel_task_2",
        python_callable=parallel_task_2
    )

    task_4 = PythonOperator(
        task_id="end_pipeline",
        python_callable=end_task
    )

    task_1 >> [task_2, task_3] >> task_4