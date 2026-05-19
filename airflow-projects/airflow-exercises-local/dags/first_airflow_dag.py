from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def welcome():
    print("Welcome to Airflow Data Engineering Pipeline")

with DAG(
    dag_id="first_airflow_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning"]
) as dag:

    task_1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task_2 = BashOperator(
        task_id="print_working_directory",
        bash_command="pwd"
    )

    task_3 = PythonOperator(
        task_id="welcome_task",
        python_callable=welcome
    )

    task_1 >> task_2 >> task_3