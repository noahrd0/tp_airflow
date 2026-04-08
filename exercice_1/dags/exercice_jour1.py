from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_date():
    print(datetime.now().strftime('%Y-%m-%d'))

with DAG(
    dag_id='exercice_jour1',
    schedule_interval='*/5 * * * *', # every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Début du workflow"',
    )

    date_task = PythonOperator(
        task_id='date_task',
        python_callable=print_date,
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo "Fin du workflow"',
    )

    start_task >> date_task >> end_task