import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
API_PARAMS = {
    "limit": 20,
    "refine": 'date_heure:2026'
}


def make_session():
    """Crée une session HTTP qui réessaie automatiquement en cas d'erreur temporaire.

    Une Session réutilise la connexion entre les appels (plus rapide que requests.get)
    et le HTTPAdapter y attache la logique de retry, ici pour toutes les URL en https.
    """
    retry = Retry(
        total=3,
        backoff_factor=1,  # délai de retry -> backoff * 2^(n - 1)
        status_forcelist=[429, 500, 502, 503, 504],  # codes sur les lesques on fait un retry
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dag(
    dag_id="eco2mix_ingestion",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def eco2mix_ingestion():

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
    def fetch(data_interval_start=None, data_interval_end=None):
        print(data_interval_start, data_interval_end)
        with make_session() as session:
            response = session.get(API_URL, params=API_PARAMS, timeout=10)
            response.raise_for_status()
            return response.json()

    @task
    def show_data(data: dict):
        print(data)

    show_data(fetch())


eco2mix_ingestion()
