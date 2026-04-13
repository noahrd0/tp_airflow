from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import time

default_args = {
    'owner': 'airflow',
}

def create_file():
    import time
    time.sleep(15)
    with open('/tmp/go.txt', 'w') as f:
        f.write('Go Airflow!')

def wait_until_file_exists():
    import os
    while not os.path.exists('/tmp/go.txt'):
        print("Fichier non trouvé, attente...")
        time.sleep(5)

def process_file():
    with open('/tmp/go.txt', 'r') as f:
        content = f.read()
    print(f"Contenu du fichier: {content}")

with DAG(
    dag_id='exercice_jour2_wait_until',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['exercice', 'sensor', 'file-waiting'],
) as dag:
    
    create_file_task = PythonOperator(
        task_id='create_file',
        python_callable=create_file,
    )
    
    wait_file = PythonOperator(
        task_id='wait_until_file_exists',
        python_callable=wait_until_file_exists,
    )
    
    process_file_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
    )
    
    [create_file_task, wait_file] >> process_file_task