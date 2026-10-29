FROM apache/airflow:3.3.2

# Versions de dbt : à garder alignées avec 01-docker-terraform/pipeline/uv.lock
ARG DBT_CORE_VERSION=1.12.6
ARG DBT_POSTGRES_VERSION=1.11.0

# dbt vit dans son propre venv : ses dépendances (protobuf, pydantic...) ne se mélangent
# pas à celles d'Airflow. Le venv est sous AIRFLOW_HOME, où l'utilisateur "airflow" a le
# droit d'écrire : pas besoin de passer en root.
# On n'ajoute volontairement pas le venv au PATH, sinon son python masquerait celui d'Airflow.
# Les DAGs appellent dbt par son chemin complet : ${DBT_VENV}/bin/dbt
ENV DBT_VENV=/opt/airflow/dbt-venv
RUN python -m venv "${DBT_VENV}" \
 && "${DBT_VENV}/bin/pip" install --no-cache-dir \
      "dbt-core==${DBT_CORE_VERSION}" \
      "dbt-postgres==${DBT_POSTGRES_VERSION}"

# Le build échoue ici si dbt est cassé, plutôt qu'au premier run d'un DAG
RUN "${DBT_VENV}/bin/dbt" --version
