import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url.rstrip("/")
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        # S'assurer que le chemin commence par /
        if not path.startswith("/"):
            path = "/" + path

        # Paramètres de base
        query_params = f"op={op}&user.name={self.user}"

        # Ajouter les paramètres supplémentaires
        for key, value in params.items():
            query_params += f"&{key}={value}"

        return f"{self.base_url}{path}?{query_params}"

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Crée un répertoire (et ses parents) dans HDFS.
        Retourne True si succès, lève une exception sinon.
        """
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url, timeout=30)
        response.raise_for_status()

        result = response.json()
        success = result.get("boolean", False)

        if not success:
            raise RuntimeError(f"Impossible de créer le répertoire HDFS : {hdfs_path}")

        logger.info("Répertoire HDFS créé : %s", hdfs_path)
        return True

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS.
        Retourne le chemin HDFS du fichier uploadé.

        Rappel : WebHDFS upload = 2 étapes
          1. PUT sur le NameNode (allow_redirects=False) → récupère l'URL de redirection
          2. PUT sur le DataNode avec le contenu binaire du fichier
        """
        # Étape 1 : Initier l'upload sur le NameNode (retourne un 307 Redirect)
        init_url = self._url(hdfs_path, "CREATE", overwrite="true")
        init_response = requests.put(init_url, allow_redirects=False, timeout=30)

        # WebHDFS répond 307 (Temporary Redirect) avec l'URL du DataNode
        if init_response.status_code != 307:
            raise RuntimeError(
                f"Étape 1 upload HDFS : code inattendu {init_response.status_code} "
                f"(attendu 307). Body: {init_response.text}"
            )

        datanode_url = init_response.headers["Location"]
        logger.info("Redirection DataNode : %s", datanode_url)

        # Étape 2 : Envoyer le contenu binaire au DataNode
        with open(local_file_path, "rb") as f:
            upload_response = requests.put(
                datanode_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600,  # Timeout long pour les gros fichiers
            )

        # 201 Created = succès
        if upload_response.status_code != 201:
            raise RuntimeError(
                f"Étape 2 upload HDFS : code inattendu {upload_response.status_code} "
                f"(attendu 201). Body: {upload_response.text}"
            )

        logger.info("Fichier uploadé dans HDFS : %s", hdfs_path)
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        """
        Lit le contenu d'un fichier HDFS.
        Retourne les données brutes (bytes).
        """
        url = self._url(hdfs_path, "OPEN")

        # allow_redirects=True : requests suit automatiquement la redirection vers le DataNode
        response = requests.get(url, allow_redirects=True, timeout=300)
        response.raise_for_status()

        logger.info(
            "Fichier lu depuis HDFS : %s (%d octets)",
            hdfs_path,
            len(response.content),
        )
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier ou répertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url, timeout=15)

        # 200 = existe, 404 = n'existe pas
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            response.raise_for_status()
            return False

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un répertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        file_statuses = data.get("FileStatuses", {}).get("FileStatus", [])

        logger.info(
            "Répertoire HDFS %s : %d élément(s) trouvé(s)",
            hdfs_path,
            len(file_statuses),
        )
        return file_statuses