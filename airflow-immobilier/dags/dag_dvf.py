from __future__ import annotations
import io
import logging
import os
import tempfile
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL = (
    "https://www.data.gouv.fr/fr/datasets/r/90a98de0-f562-4328-aa16-fe0dd1dca60f"
)
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",  
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():
    """DAG principal du pipeline DVF."""

    # Vérification des sources
    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        """
        Vérifie la disponibilité des sources de données et de l'infrastructure.

        Retourne un dictionnaire de la forme :
            {
                "dvf_api": True,
                "hdfs": True,
                "timestamp": "2024-01-15T06:00:00"
            }

        Lève une AirflowException si une source critique est indisponible.
        """
        statuts = {}

        # 1 : Vérifier l'API data.gouv.fr
        try:
            resp = requests.get(DVF_URL, stream=True, timeout=15, allow_redirects=True)
            resp.close() 
            statuts["dvf_api"] = resp.status_code < 400
        except requests.RequestException as exc:
            logger.warning("API DVF inaccessible : %s", exc)
            statuts["dvf_api"] = False

        # 2 : Vérifier la disponibilité de HDFS via WebHDFS
        try:
            hdfs_check_url = f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}"
            resp_hdfs = requests.get(hdfs_check_url, timeout=10)
            statuts["hdfs"] = resp_hdfs.status_code == 200
        except requests.RequestException as exc:
            logger.warning("HDFS inaccessible : %s", exc)
            statuts["hdfs"] = False

        # 3 : Logger l'état de chaque service
        logger.info("Statut API DVF  : %s", "OK" if statuts["dvf_api"] else "KO")
        logger.info("Statut HDFS     : %s", "OK" if statuts["hdfs"] else "KO")

        # 4 : Lever une exception si HDFS est inaccessible
        if not statuts["dvf_api"]:
            logger.warning(
                "API data.gouv.fr inaccessible depuis le conteneur. "
                "Vérifiez la connectivité réseau de Docker. "
                "Le pipeline continue — le téléchargement tentera quand même."
            )
        if not statuts["hdfs"]:
            raise AirflowException(
                "Le cluster HDFS (WebHDFS) est inaccessible. "
                "Pipeline interrompu."
            )

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    # Téléchargement du CSV DVF
    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        """
        Utilise le fichier CSV DVF local au lieu de le télécharger.
        Le fichier doit être placé dans data/dvf_2023.csv
        (monté dans le conteneur via le volume ./data:/opt/airflow/data).

        Retourne :
            str : chemin local du fichier CSV
        """
        # Chemin du fichier local monté dans le conteneur
        local_path = "/opt/airflow/data/dvf_2023.csv"

        # Vérifier que le fichier existe
        if not os.path.exists(local_path):
            raise AirflowException(
                f"Fichier introuvable : {local_path}\n"
                "Vérifiez que data/dvf_2023.csv existe dans votre projet "
                "et que le volume est bien monté dans docker-compose.yaml."
            )

        taille = os.path.getsize(local_path)
        if taille < 1000:
            raise AirflowException(
                f"Fichier suspect : seulement {taille} octets."
            )

        logger.info(
            "Fichier DVF local trouvé : %s (%.1f Mo)",
            local_path,
            taille / (1024 * 1024),
        )

        return local_path

    # Stockage dans HDFS
    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        """
        Uploade le fichier CSV local vers HDFS (zone raw / data lake).

        Paramètre :
            local_path (str) : chemin du fichier CSV local

        Retourne :
            str : chemin HDFS du fichier uploadé (ex: /data/dvf/raw/dvf_2024.csv)
        """
        annee = datetime.now().year
        hdfs_filename = f"dvf_{annee}.csv"
        hdfs_file_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"

        # 1 : Créer le répertoire HDFS si nécessaire
        mkdirs_url = (
            f"{WEBHDFS_BASE_URL}{HDFS_RAW_PATH}/"
            f"?op=MKDIRS&user.name={WEBHDFS_USER}"
        )
        mkdirs_resp = requests.put(mkdirs_url, timeout=30)
        mkdirs_resp.raise_for_status()

        if not mkdirs_resp.json().get("boolean", False):
            raise AirflowException(f"Impossible de créer le répertoire HDFS : {HDFS_RAW_PATH}")
        logger.info("Répertoire HDFS prêt : %s", HDFS_RAW_PATH)

        # 2 : Initier l'upload 
        create_url = (
            f"{WEBHDFS_BASE_URL}{hdfs_file_path}"
            f"?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
        )
        init_resp = requests.put(create_url, allow_redirects=False, timeout=30)

        if init_resp.status_code != 307:
            raise AirflowException(
                f"Étape 1 HDFS upload : attendu 307, reçu {init_resp.status_code}"
            )
        datanode_url = init_resp.headers["Location"]
        logger.info("URL DataNode récupérée : %s", datanode_url)

        # 3 : Envoyer le fichier au DataNode 
        with open(local_path, "rb") as f:
            upload_resp = requests.put(
                datanode_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600,
            )

        if upload_resp.status_code != 201:
            raise AirflowException(
                f"Étape 2 HDFS upload : attendu 201, reçu {upload_resp.status_code}"
            )

        # 4 : On garde le fichier source local
        logger.info("Fichier source conservé : %s", local_path)

        # 5 : Logger et retourner le chemin HDFS
        logger.info("Fichier stocké dans HDFS : %s", hdfs_file_path)
        return hdfs_file_path

    # Traitement et agrégation des données
    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        """
        Lit le CSV depuis HDFS, filtre les appartements parisiens,
        calcule le prix au m² et agrège par arrondissement.

        Filtres :
            - Type de bien : "Appartement" uniquement
            - Codes postaux : 75001 à 75020
            - Surface : entre 9 m² et 500 m²
            - Prix : supérieur à 10 000 EUR
            - Nature de la mutation : "Vente"

        Retourne :
            dict avec deux clés :
                "agregats"       : liste de dicts (une ligne par arrondissement)
                "stats_globales" : dict avec les statistiques globales Paris
        """
        # 1 et 2 : Lire le CSV depuis HDFS par chunks pour éviter l'OOM
        open_url = (
            f"{WEBHDFS_BASE_URL}{hdfs_path}"
            f"?op=OPEN&user.name={WEBHDFS_USER}"
        )
        response = requests.get(open_url, allow_redirects=True, timeout=300, stream=True)
        response.raise_for_status()

        codes_paris = [f"750{i:02d}" for i in range(1, 10)] + [f"75{i}" for i in range(10, 21)]

        chunks_filtres = []
        total_lignes = 0
        premiere_chunk = True
        chunk_size = 50_000  

        for chunk in pd.read_csv(
            response.raw,
            sep=",",
            low_memory=False,
            dtype={"code_postal": str},
            chunksize=chunk_size,
        ):
            # Normaliser les colonnes sur le premier chunk uniquement
            chunk.columns = (
                chunk.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_", regex=False)
                .str.replace("'", "_", regex=False)
            )

            if premiere_chunk:
                logger.info("Colonnes disponibles : %s", list(chunk.columns))
                logger.info("Exemple nature_mutation : %s", chunk["nature_mutation"].dropna().unique()[:5].tolist())
                logger.info("Exemple type_local      : %s", chunk["type_local"].dropna().unique()[:5].tolist())
                logger.info("Exemple code_postal     : %s", chunk["code_postal"].dropna().unique()[:10].tolist())
                premiere_chunk = False

            total_lignes += len(chunk)

            # Convertir les décimales (DVF utilise "," comme séparateur décimal)
            chunk["valeur_fonciere"] = pd.to_numeric(
                chunk["valeur_fonciere"].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
            chunk["surface_reelle_bati"] = pd.to_numeric(
                chunk["surface_reelle_bati"].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )

            # Appliquer tous les filtres sur le chunk
            chunk = chunk[chunk["nature_mutation"].str.strip() == "Vente"]
            chunk = chunk[chunk["type_local"].str.strip() == "Appartement"]
            chunk = chunk[chunk["code_postal"].isin(codes_paris)]
            chunk = chunk[chunk["surface_reelle_bati"].between(9, 500)]
            chunk = chunk[chunk["valeur_fonciere"] > 10_000]
            chunk = chunk[chunk["surface_reelle_bati"] > 0]

            if not chunk.empty:
                chunks_filtres.append(chunk)

        logger.info("Nombre de lignes lues au total : %d", total_lignes)

        if not chunks_filtres:
            logger.warning("Aucune donnée après filtrage — retour d'agrégats vides.")
            return {"agregats": [], "stats_globales": {}}

        # Assembler tous les chunks filtrés en un seul DataFrame
        df = pd.concat(chunks_filtres, ignore_index=True)
        logger.info("Nombre de lignes après filtrage : %d", len(df))

        # 4 : Calculer le prix au m²
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        # 5 : Extraire l'arrondissement depuis le code postal
        df["arrondissement"] = df["code_postal"].apply(
            lambda cp: int(cp[3:]) if cp.startswith("75") and len(cp) == 5 else None
        )
        df = df.dropna(subset=["arrondissement"])
        df["arrondissement"] = df["arrondissement"].astype(int)

        # Ajouter année et mois de mutation
        df["date_mutation"] = pd.to_datetime(df.get("date_mutation"), errors="coerce")
        df["annee"] = df["date_mutation"].dt.year.fillna(datetime.now().year).astype(int)
        df["mois"]  = df["date_mutation"].dt.month.fillna(datetime.now().month).astype(int)

        # 6 : Agréger par (code_postal, arrondissement, annee, mois)
        agregat_df = (
            df.groupby(["code_postal", "arrondissement", "annee", "mois"])
            .agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean"),
            )
            .reset_index()
        )

        agregats = agregat_df.to_dict(orient="records")
        logger.info("Nombre d'agrégats calculés : %d", len(agregats))

        # 7 : Statistiques globales Paris
        stats_globales = {
            "annee": int(df["annee"].mode()[0]) if not df.empty else datetime.now().year,
            "mois": int(df["mois"].mode()[0]) if not df.empty else datetime.now().month,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris": float(df["prix_m2"].median()),
            "prix_m2_moyen_paris": float(df["prix_m2"].mean()),
            "arrdt_plus_cher": int(
                agregat_df.loc[agregat_df["prix_m2_median"].idxmax(), "arrondissement"]
            ) if not agregat_df.empty else None,
            "arrdt_moins_cher": int(
                agregat_df.loc[agregat_df["prix_m2_median"].idxmin(), "arrondissement"]
            ) if not agregat_df.empty else None,
            "surface_mediane": float(df["surface_reelle_bati"].median()),
        }

        logger.info(
            "Stats Paris — médiane : %.0f €/m² | transactions : %d",
            stats_globales["prix_m2_median_paris"],
            stats_globales["nb_transactions_total"],
        )

        # 8 : Retourner le dictionnaire résultat
        return {"agregats": agregats, "stats_globales": stats_globales}

    # Tâche 5 — Insertion dans PostgreSQL 
    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        """
        Insère les données agrégées dans PostgreSQL via UPSERT (idempotent).

        Paramètre :
            resultats (dict) : résultat de traiter_donnees()

        Retourne :
            int : nombre de lignes insérées ou mises à jour
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 1 : Récupérer les données depuis le dictionnaire
        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        if not agregats:
            logger.warning("Aucun agrégat à insérer.")
            return 0

        # 2 : Requête UPSERT pour prix_m2_arrondissement
        upsert_query = """
            INSERT INTO prix_m2_arrondissement
                (code_postal, arrondissement, annee, mois,
                 prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
                 nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                prix_m2_median  = EXCLUDED.prix_m2_median,
                prix_m2_min     = EXCLUDED.prix_m2_min,
                prix_m2_max     = EXCLUDED.prix_m2_max,
                nb_transactions = EXCLUDED.nb_transactions,
                surface_moyenne = EXCLUDED.surface_moyenne,
                updated_at      = NOW();
        """

        # 3 : Exécuter l'UPSERT pour chaque arrondissement
        nb_inseres = 0
        for row in agregats:
            params = (
                str(row["code_postal"]),
                int(row["arrondissement"]),
                int(row["annee"]),
                int(row["mois"]),
                float(row["prix_m2_moyen"]),
                float(row["prix_m2_median"]),
                float(row["prix_m2_min"]),
                float(row["prix_m2_max"]),
                int(row["nb_transactions"]),
                float(row["surface_moyenne"]),
            )
            hook.run(upsert_query, parameters=params)
            nb_inseres += 1

        logger.info("Arrondissements insérés/mis à jour : %d", nb_inseres)

        # 4 : Insérer les statistiques globales dans stats_marche
        if stats_globales:
            stats_query = """
                INSERT INTO stats_marche
                    (annee, mois, nb_transactions_total,
                     prix_m2_median_paris, prix_m2_moyen_paris,
                     arrdt_plus_cher, arrdt_moins_cher, surface_mediane)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (annee, mois) DO UPDATE SET
                    nb_transactions_total = EXCLUDED.nb_transactions_total,
                    prix_m2_median_paris  = EXCLUDED.prix_m2_median_paris,
                    prix_m2_moyen_paris   = EXCLUDED.prix_m2_moyen_paris,
                    arrdt_plus_cher       = EXCLUDED.arrdt_plus_cher,
                    arrdt_moins_cher      = EXCLUDED.arrdt_moins_cher,
                    surface_mediane       = EXCLUDED.surface_mediane,
                    date_calcul           = NOW();
            """
            hook.run(
                stats_query,
                parameters=(
                    int(stats_globales.get("annee", datetime.now().year)),
                    int(stats_globales.get("mois", datetime.now().month)),
                    int(stats_globales.get("nb_transactions_total", 0)),
                    float(stats_globales.get("prix_m2_median_paris", 0)),
                    float(stats_globales.get("prix_m2_moyen_paris", 0)),
                    stats_globales.get("arrdt_plus_cher"),
                    stats_globales.get("arrdt_moins_cher"),
                    float(stats_globales.get("surface_mediane", 0)),
                ),
            )
            logger.info("Statistiques globales insérées dans stats_marche.")

        # 5 : Retourner le nombre de lignes traitées
        return nb_inseres

    # Génération du rapport de classement
    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        """
        Génère un rapport texte classant les arrondissements parisiens
        par prix médian au m² décroissant.

        Paramètre :
            nb_inseres (int) : nombre de lignes insérées (depuis inserer_postgresql)

        Retourne :
            str : rapport formaté
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Récupérer l'année et le mois les plus récents présents dans les données
        annee_mois = hook.get_first("""
            SELECT annee, mois FROM prix_m2_arrondissement
            ORDER BY annee DESC, mois DESC LIMIT 1;
        """)

        if not annee_mois:
            logger.warning("Table prix_m2_arrondissement vide.")
            return "Aucune donnée disponible."

        annee, mois = annee_mois
        logger.info("Rapport sur les données : %d/%d", mois, annee)

        # 1 : Requête SQL de ranking
        query = """
            SELECT
                arrondissement,
                prix_m2_median,
                prix_m2_moyen,
                nb_transactions,
                surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s
            ORDER BY prix_m2_median DESC
            LIMIT 20;
        """

        # 2 : Exécuter la requête
        records = hook.get_records(query, parameters=(annee, mois))

        if not records:
            logger.warning("Aucun enregistrement trouvé pour %d/%d.", mois, annee)
            return "Aucune donnée disponible pour cette période."

        # 3 : Formater les résultats en tableau lisible
        separateur = "-" * 72
        entete = (
            f"{'Arrdt':>6} | {'Médiane (€/m²)':>15} | "
            f"{'Moyenne (€/m²)':>15} | {'Transactions':>13} | {'Surface moy.':>12}"
        )

        lignes = [
            f"\n{'═' * 72}",
            f"  CLASSEMENT DES ARRONDISSEMENTS PARISIENS — {mois:02d}/{annee}",
            f"  Lignes insérées : {nb_inseres}",
            f"{'═' * 72}",
            entete,
            separateur,
        ]

        noms_arrdt = {
            1: "1er", 2: "2e", 3: "3e", 4: "4e", 5: "5e",
            6: "6e", 7: "7e", 8: "8e", 9: "9e", 10: "10e",
            11: "11e", 12: "12e", 13: "13e", 14: "14e", 15: "15e",
            16: "16e", 17: "17e", 18: "18e", 19: "19e", 20: "20e",
        }

        for i, (arrdt, median, moyen, nb_tx, surf) in enumerate(records, 1):
            nom = noms_arrdt.get(arrdt, f"{arrdt}e")
            ligne = (
                f"  {nom:>4} | {median:>13,.0f}  | "
                f"{moyen:>13,.0f}  | {nb_tx:>12,} | {surf:>10.1f} m²"
            )
            lignes.append(ligne)

        lignes.append(separateur)
        rapport = "\n".join(lignes)

        # 4 : Logger le rapport
        logger.info(rapport)

        # 5 : Retourner le rapport
        return rapport

    # Instanciation et enchaînement des tâches
    t_verif    = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs     = stocker_hdfs_raw(t_download)
    t_traiter  = traiter_donnees(t_hdfs)
    t_pg       = inserer_postgresql(t_traiter)
    t_rapport  = generer_rapport(t_pg)

    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)


# Instancier le DAG
pipeline_dvf()