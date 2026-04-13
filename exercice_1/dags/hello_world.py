from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id='hello_world',
    schedule_interval='*/5 * * * *', # every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    bash_task = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Hello from Bash!"',
    )

    hello_task >> bash_task