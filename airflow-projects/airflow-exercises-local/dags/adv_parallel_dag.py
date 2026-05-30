from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_customer_data():
    print("Extracting customer data")

def extract_orders_data():
    print("Extracting orders data")

def final_pipeline_status():
    print("Pipeline completed successfully")

airflow_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='Adv_Parallel_DAG',
    default_args=airflow_default_args,
    description='Advanced Parallel DAG Example',
    start_date=datetime(2026, 2, 13),
    schedule="*/2 * * * *",
    catchup=False,
    tags=['dev']
)

start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Pipeline Started"',
    dag=dag,
)

customer_task = PythonOperator(
    task_id='customer_extraction',
    python_callable=extract_customer_data,
    dag=dag,
)

orders_task = PythonOperator(
    task_id='orders_extraction',
    python_callable=extract_orders_data,
    dag=dag,
)

final_task = PythonOperator(
    task_id='final_pipeline_status',
    python_callable=final_pipeline_status,
    dag=dag,
)

start_task >> [customer_task, orders_task] >> final_task