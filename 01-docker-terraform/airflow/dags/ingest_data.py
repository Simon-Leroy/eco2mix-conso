import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
RAW_DIR = Path("/opt/airflow/data/raw/eco2mix")
SQL_DIR = Path(__file__).parent / "sql"
POSTGRES_CONN_ID = "eco2mix_postgres"

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


PAGE_SIZE = 100


def fetch_all(session, where):
    """Récupère toutes les lignes qui correspondent au filtre, page par page"""
    records = []
    offset = 0
    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "where": where,
            "order_by": "date_heure",
        }
        response = session.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        records.extend(payload["results"])
        offset += PAGE_SIZE
        if offset >= payload["total_count"]:
            break
    return records


@dag(
    dag_id="eco2mix_ingestion",
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def eco2mix_ingestion():

    @task
    def create_table():
        sql = (SQL_DIR / "create_raw_eco2mix.sql").read_text()
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
    def fetch(data_interval_start=None, data_interval_end=None):
        start = data_interval_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = data_interval_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        where = f'date_heure >= "{start}" AND date_heure < "{end}"'

        with make_session() as session:
            records = fetch_all(session, where)

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = RAW_DIR / f"{data_interval_start:%Y-%m-%dT%H-%M}.json"
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(records, ensure_ascii=False))
        tmp_path.replace(path)

        print(f"{len(records)} lignes écrites dans {path}")
        return str(path)

    @task
    def load(path: str):
        records = json.loads(Path(path).read_text())
        rows = [
            (r["date_heure"], r["date"], r["heure"], json.dumps(r, ensure_ascii=False))
            for r in records
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.insert_rows(
            table="raw_eco2mix",
            rows=rows,
            target_fields=["date_heure", "date_locale", "heure_locale", "payload"],
            replace=True,
            replace_index=["date_heure", "date_locale", "heure_locale"],
            commit_every=len(rows),
        )
        print(f"{len(rows)} lignes chargées depuis {path}")

    create = create_table()
    path = fetch()
    loaded = load(path)
    create >> loaded


eco2mix_ingestion()
