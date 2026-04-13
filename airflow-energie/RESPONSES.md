## Q1 — Docker Executor
### Le docker-compose.yaml utilise LocalExecutor . Expliquez la différence entre LocalExecutor , CeleryExecutor et KubernetesExecutor . Dans quel contexte de production RTE devrait-il utiliser chacun ? Quelles sont les implications en termes de scalabilité et de ressources ?

LocalExecutor permet de lancer plusieurs tâches en même temps sur une seule machine. CeleryExecutor peut lancer plusieurs tâches sur plusieurs workers en simultané. Enfin, KubernetesExecutor lance un pod éphémère pour exécuter une tâche.

RTE pourrait utiliser LocalExecutor si la production est légère et repose sur un serveur unique, avec une scalabilité limitée aux ressources de la machine.
Ils pourraient utiliser CeleryExecutor en cas de forte charge, avec plusieurs workers répartis sur plusieurs machines, permettant une scalabilité horizontale mais nécessitant la gestion d’un broker (Redis, RabbitMQ).
Enfin, KubernetesExecutor serait adapté à un environnement cloud-native, avec une forte isolation des dépendances et une scalabilité dynamique : les ressources sont allouées à la demande pour chaque tâche, ce qui optimise les coûts mais demande une infrastructure Kubernetes complexe à maintenir.

## Q2 — Volumes Docker et persistance des DAGs Le volume
### ./dags:/opt/airflow/dags permet de modifier les DAGs sans redémarrer le conteneur. Expliquez le mécanisme sous-jacent (bind mount vs volume nommé). Que se passerait-il si on supprimait ce mapping ? Quel serait l’impact en production sur un cluster Airflow multi-nœuds (plusieurs workers) ?

Le mécanisme derrière le bind mount permet de relier un dossier de la machine avec un conteneur, contrairement à un volume nommé qui laisse Docker gérer les fichiers via un stockage interne (/var/lib/docker).

Si on venait à supprimer ce mapping, les DAGs ne seraient plus synchronisés avec la machine hôte ; chaque modification entraînerait alors un redémarrage ou un rebuild du conteneur pour resynchroniser.

Sur un cluster Airflow multi-nœuds en production, chaque worker pourrait avoir une version différente des DAGs.

## Q3 — Idempotence et catchup
### Le DAG a catchup=False . Expliquez ce que ferait Airflow si catchup=True et que le DAG est activé aujourd’hui avec un start_date au 1er janvier 2024. Qu’est-ce que l’idempotence d’un DAG et pourquoi est-ce critique pour un pipeline de données énergétiques ? Comment rendre les fonctions collecter_* idempotentes ?

Si le DAG est active avec catchup=True il relancerait toutes les execution entre start_date et aujourd'hui afin de rattraper toutes les executions qu'il n'a pas faites.

L’idempotence permet d’exécuter plusieurs fois une même tâche sans modifier le résultat final ni créer de doublons. C’est essentiel pour un pipeline de données énergétiques, car une erreur ou une duplication pourrait fausser les analyses (consommation, production, prévisions).

Pour rendre les fonctions de collecte idempotentes on peut verifier l'existance avec l'insertion, utiliser des clés uniques et des contraintes en base de données.

## Q4 — Timezone et données temps-réel
### L’API éCO2mix retourne des données horodatées. Pourquoi le paramètre timezone=Europe/Paris est-il essentiel dans le contexte RTE ? Que peut-il se passer lors du passage à l’heure d’été (dernier dimanche de mars) si la timezone n’est pas correctement gérée dans le scheduler Airflow et dans les requêtes API ? Donnez un exemple concret de données corrompues ou manquantes.

La timezone est essentielle car RTE travaille en heure locale pour suivre les données de production et de consommation d’éCO2mix. Elle permet aussi d’éviter le saut ou la répétition d’une tâche lors d’un changement d’heure (été ou hiver). Para exemple, si lors d’un changement d’heure on passe de 03:00 à 02:01 et que la timezone n’est pas configurée, on peut se retrouver avec des données de consommation en double.

![Interface Airflow UI avec le DAG energie_meteo_dag visible et en état success](/screen_livrable/image.png)
![Vue Graph du DAG montrant les 5 tâches et leurs dépendances](/screen_livrable/image-1.png)
![Logs d’une exécution réussie de generer_rapport_energie avec le tableau affiché](/screen_livrable/image-2.png)
![Onglet XCom de la tâche analyser_correlation montrant le dictionnaire d’alertes](/screen_livrable/image-3.png)
![Contenu du fichier JSON généré (via docker compose exec ... cat /tmp/rapport_energie_*.json )](/screen_livrable/image-4.png)
