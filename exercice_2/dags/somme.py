from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import random

default_args = {
    'owner': 'airflow',
}

def generate_random_number(**context):
    nb_tab = []
    for i in range(5):
        number = random.randint(1, 100)
        nb_tab.append(number)
    return nb_tab

def calculate_sum(**context):
    ti = context['ti']
    nb_tab = ti.xcom_pull(task_ids='generate_number')
    total_sum = sum(nb_tab)
    print(f"{nb_tab} = {total_sum}")

with DAG(
    dag_id='exercice_jour2_somme',
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
    
    calculate_sum = PythonOperator(
        task_id='calculate_sum',
        python_callable=calculate_sum,
    )
    
    generate_number >> calculate_sum