from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_world",
    schedule=None,  # lancement manuel uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def hello_world():
    @task
    def say_hello() -> str:
        print("Hello world !")
        return "Simon"

    @task
    def greet(name: str):
        print(f"Bonjour {name}, ton premier DAG Airflow fonctionne.")

    # say_hello s'exécute d'abord, sa valeur de retour est passée à greet (via XCom)
    greet(say_hello())


hello_world()
