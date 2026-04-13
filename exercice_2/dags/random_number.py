from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import random

default_args = {
    'owner': 'airflow',
}

def generate_random_number(**context):
    number = random.randint(1, 100)
    context['ti'].xcom_push(key='random_number', value=number)
    return number

def branch_on_number(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    return 'pair' if number % 2 == 0 else 'impair'

def pair(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    print(f"Le nombre {number} est pair")

def impair(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    print(f"Le nombre {number} est impair")

with DAG(
    dag_id='exercice_jour2',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['exercice', 'branching'],
) as dag:
    
    generate_number = PythonOperator(
        task_id='generate_number',
        python_callable=generate_random_number,
    )
    
    branch = BranchPythonOperator(
        task_id='branch_on_number',
        python_callable=branch_on_number,
    )
    
    task_pair = PythonOperator(
        task_id='pair',
        python_callable=pair,
    )
    
    task_impair = PythonOperator(
        task_id='impair',
        python_callable=impair,
    )
    
    generate_number >> branch >> [task_pair, task_impair]