from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def get_environment():

    env = Variable.get("environment")

    print(f"Current Environment: {env}")

with DAG(
    dag_id="variables_dag",
    start_date=datetime(2026, 2, 14),
    schedule="@daily",
    catchup=False,
    tags=["variables"]
) as dag:

    environment_task = PythonOperator(
        task_id="get_environment_task",
        python_callable=get_environment
    )