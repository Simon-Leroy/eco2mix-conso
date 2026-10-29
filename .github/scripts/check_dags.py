"""Charge tous les DAGs du dossier et échoue si l'un d'eux ne s'importe pas."""
import sys

from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = "airflow/dags"

# Comme le fait Airflow en vrai : le dossier des DAGs est dans le chemin d'import,
# ce qui permet `from common.eco2mix_api import ...`.
sys.path.insert(0, DAGS_FOLDER)

bag = DagBag(dag_folder=DAGS_FOLDER)

if bag.import_errors:
    for path, error in bag.import_errors.items():
        print(f"ERREUR d'import dans {path}:\n{error}")
    sys.exit(1)

print(f"OK : {len(bag.dags)} DAG(s) chargé(s) : {sorted(bag.dags)}")
