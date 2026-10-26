import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from common.eco2mix_api import fetch_all, make_session

API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
RAW_DIR = Path("/opt/airflow/data/raw/eco2mix")
SQL_DIR = Path(__file__).parent / "sql"
POSTGRES_CONN_ID = "eco2mix_postgres"


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
            records = fetch_all(session, API_URL, where)

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
        return len(rows)

    @task
    def check(expected_rows: int, data_interval_start=None, data_interval_end=None):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        in_db = hook.get_first(
            "SELECT count(*) FROM raw_eco2mix WHERE date_heure >= %s AND date_heure < %s",
            parameters=(data_interval_start, data_interval_end),
        )[0]

        if in_db != expected_rows:
            raise ValueError(f"Attendu {expected_rows} en base, trouvé {in_db}")
        print(f"OK : {in_db} lignes en base pour la période")

    create = create_table()
    path = fetch()
    loaded = load(path)
    create >> loaded
    check(loaded)


eco2mix_ingestion()
