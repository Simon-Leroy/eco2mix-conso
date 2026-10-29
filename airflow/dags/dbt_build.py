from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# Chemin du projet dbt dans le conteneur (monté par docker-compose.yml, en lecture seule)
DBT_PROJECT_DIR = "/opt/airflow/dbt_eco2mix"


@dag(
    dag_id="dbt_build",
    schedule=None,  # lancé à la main pour l'instant, branché sur l'ingestion à l'étape suivante
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Une seule exécution à la fois : deux dbt en parallèle réécriraient les mêmes tables
    max_active_runs=1,
    tags=["dbt"],
)
def dbt_build():

    # `dbt build` exécute les modèles ET leurs tests, dans l'ordre des dépendances :
    # si un test du staging échoue, les modèles qui en dépendent ne sont pas construits.
    # $DBT_VENV est défini dans le Dockerfile (venv isolé d'Airflow).
    BashOperator(
        task_id="dbt_build",
        bash_command=(
            '"$DBT_VENV/bin/dbt" build '
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
        # Un échec de test dbt n'a pas de raison de disparaître en réessayant : pas de retry
        retries=0,
        execution_timeout=timedelta(minutes=30),
    )


dbt_build()
