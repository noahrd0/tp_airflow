from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor  # Remplacé WebHdfsSensor par BashSensor
import random
import subprocess
import logging

default_args = {
    'owner': 'airflow',
}

def generer_logs_journaliers(**context):
    """
    Génère 1000 lignes de logs Apache pour la date d'exécution du DAG.
    Sauvegarde dans /tmp/access_<YYYY-MM-DD>.log.
    Retourne le chemin du fichier (stocké dans XCom pour les tâches suivantes).
    """
    # La date logique d'exécution (pas forcément aujourd'hui si catchup=True)
    execution_date = context["ds"] # Format YYYY-MM-DD
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    try:
        result = subprocess.run(
            ["python3", script_path, execution_date, "1000", fichier_sortie],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"Script output: {result.stdout}")
        if not os.path.exists(fichier_sortie):
            raise FileNotFoundError(f"Fichier de logs non trouvé: {fichier_sortie}")
        taille = os.path.getsize(fichier_sortie)
        logging.info(f"Fichier généré: {fichier_sortie} (taille: {taille} octets)")
        return fichier_sortie
    except subprocess.CalledProcessError as e:
        logging.error(f"Erreur lors de l'exécution du script: {e.stderr}")
        raise

SEUIL_ERREUR_PCT = 5.0 # Seuil d'alerte : 5% d'erreurs HTTP
def brancher_selon_taux_erreur(**context):
    """
    Lit le taux d'erreur calculé par analyser_logs_hdfs.
    Retourne le task_id de la branche à exécuter.
    """
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    try:
        with open(fichier_taux, "r") as f:
            erreurs, total = map(int, f.read().strip().split())
        taux_pct = (erreurs / total) * 100 if total > 0 else 0
        logging.info(f"Taux d'erreur: {taux_pct:.2f}%")
        if taux_pct > SEUIL_ERREUR_PCT:
            return "alerter_equipe_ops"
        else:
            return "archiver_rapport_ok"
    except FileNotFoundError:
        logging.error(f"Fichier {fichier_taux} introuvable.")
        raise

def alerter_equipe_ops(**context):
    """Simule l'envoi d'une alerte à l'équipe Ops (Slack, PagerDuty, etc.)."""
    execution_date = context["ds"]
    logging.warning(
        f"[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du {execution_date}. "
        "Vérifiez les serveurs web."
    )
    # En production : appel API Slack, PagerDuty, ou envoi email via SMTP

def archiver_rapport_ok(**context):
    """Logue que tout est nominal et archive un rapport de bonne santé."""
    execution_date = context["ds"]
    logging.info(
        f"[OK] Taux d'erreur dans les seuils normaux pour les logs du {execution_date}."
    )

with DAG(
    dag_id='logs_ecommerce_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['tp', 'ecommerce', 'logs'],
) as dag:
    generer_logs = PythonOperator(
        task_id='generer_logs',
        python_callable=generer_logs_journaliers,
    )

    t_upload = BashOperator(
        task_id="uploader_vers_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            echo "[INFO] Upload de ${FICHIER_LOCAL} vers HDFS:${CHEMIN_HDFS}"
            docker cp ${FICHIER_LOCAL} namenode:/tmp/
            docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}
            echo "[OK] Upload terminé"
        """,
    )

    t_sensor = BashSensor(
        task_id="hdfs_file_sensor",
        bash_command="""
            docker exec namenode hdfs dfs -test -e /data/ecommerce/logs/raw/access_{{ ds }}.log
        """,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    t_analyser = BashOperator(
        task_id="analyser_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            echo "[INFO] Lecture du fichier HDFS : ${CHEMIN_HDFS}"
            docker exec namenode hdfs dfs -cat "${CHEMIN_HDFS}" > /tmp/logs_analyse_${EXECUTION_DATE}.txt
            echo "[INFO] Analyse des logs..."
            echo "=== STATUS CODES ==="
            grep -oP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" [0-9]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
            | grep -oP '[0-9]+$' | sort | uniq -c | sort -rn
            echo "=== TOP 5 URLS ==="
            grep -oP '"(GET|POST) [^ ]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
            | cut -d' ' -f2 | sort | uniq -c | sort -rn | head -5
            TOTAL=$(wc -l < /tmp/logs_analyse_${EXECUTION_DATE}.txt)
            ERREURS=$(grep -cP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" (4|5)[0-9]{2}' /tmp/logs_analyse_${EXECUTION_DATE}.txt || echo 0)
            echo "=== TAUX ERREUR ==="
            echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"
            echo "${ERREURS} ${TOTAL}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        """,
    )

    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    t_archiver = BashOperator(
        task_id="archiver_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
            echo "[INFO] Déplacement HDFS : ${SOURCE} → ${DESTINATION}"
            docker exec namenode hdfs dfs -mv ${SOURCE} ${DESTINATION}
            echo "[OK] Fichier archivé dans la zone processed"
        """,
        trigger_rule="none_failed_min_one_success",
    )

    (
        generer_logs
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )