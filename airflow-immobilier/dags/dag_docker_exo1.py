from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
import os
from docker.types import Mount

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="dag_docker_operator",
    description="Exécution d'une tâche Python dans un conteneur Docker via DockerOperator",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["docker", "mise-en-pratique"],
)
def dag_docker():
    """
    DAG de démonstration du DockerOperator.

    Ce DAG lance un conteneur python:3.9-slim qui :
      - Exécute un script Python affichant un message et les variables d'env
      - Reçoit des variables d'environnement via le paramètre `environment`
      - Monte un répertoire local via le paramètre `mounts`
    """

    # lancer un conteneur Python 
    tache_docker = DockerOperator(
        task_id="executer_python_dans_docker",
        image="python:3.9-slim",
        command=(
            'python -c "'
            "import os; "
            "print('Hello from Docker!'); "
            "print('Environnement :'); "
            "print('  APP_ENV =', os.getenv('APP_ENV', 'non défini')); "
            "print('  APP_VERSION =', os.getenv('APP_VERSION', 'non défini')); "
            "import os; files = os.listdir('/data') if os.path.exists('/data') else []; "
            "print('Fichiers dans /data :', files)"
            '"'
        ),

        environment={
            "APP_ENV": "production",
            "APP_VERSION": "1.0.0",
            "PIPELINE": "dvf-immobilier",
        },
        mounts=[
            Mount(
                source="/Users/emiliedelrue/Documents/Cours/Airflow/TP3 + TPCours3/data",
                target="/data",
                type="bind",
            )
        ],

        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        retrieve_output=True,
        network_mode="tp3tpcours3_dvf-network",
        mount_tmp_dir=False,
    )

    tache_docker


dag_docker()