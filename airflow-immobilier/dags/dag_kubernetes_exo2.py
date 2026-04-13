from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="dag_kubernetes_pod_operator",
    description="Exécution d'un job Python dans un pod éphémère Kubernetes",
    schedule_interval=None,   
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["kubernetes", "k8s", "mise-en-pratique"],
)
def dag_k8s():
    ressources_pod = k8s.V1ResourceRequirements(
        requests={
            "cpu": "100m",       
            "memory": "128Mi",  
        },
        limits={
            "cpu": "200m",       
            "memory": "256Mi",   
        },
    )

    tache_k8s = KubernetesPodOperator(
        task_id="deployer_job_python_k8s",
        name="airflow-python-hello-k8s",
        namespace="default",
        image="python:3.9-slim",

        cmds=["python", "-c"],
        arguments=[
            "print('Hello K8s!'); "
            "import platform; "
            "print('Python :', platform.python_version()); "
            "print('Système :', platform.system(), platform.machine())"
        ],

        container_resources=ressources_pod,
        get_logs=True,
        is_delete_operator_pod=True,
        log_events_on_failure=True,
        config_file="/opt/airflow/.kube/config",
        in_cluster=False,
        cluster_context="docker-desktop",

    )

    tache_k8s


dag_k8s()