import json
from pathlib import Path
from datetime import datetime, timedelta
from airflow.sdk import dag, task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.eco2mix_api import make_session, fetch_all

API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records"
RAW_DIR = Path("/opt/airflow/data/raw/eco2mix_tr")
SQL_DIR = Path(__file__).parent / "sql"
POSTGRES_CONN_ID = "eco2mix_postgres"
TABLE = "raw_eco2mix_tr"


@dag(
    dag_id="eco2mix_realtime_ingestion",
    schedule=CronDataIntervalTimetable("@hourly", timezone="UTC"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Nombre de jours relus à chaque run. Les valeurs temps réel sont révisées après coup,
    # on recharge donc les derniers jours (upsert). Pour un chargement initial, lancer
    # le DAG à la main avec lookback_days=60.
    params={"lookback_days": 2},
    tags=["demo"],
)
def eco2mix_realtime_ingestion():

    @task
    def create_table():
        sql = (SQL_DIR / "create_raw_eco2mix_tr.sql").read_text()
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql)

    @task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
    def fetch(data_interval_end=None):
        lookback = int(get_current_context()["params"]["lookback_days"])
        start = (data_interval_end - timedelta(days=lookback)).strftime("%Y-%m-%dT%H:%M:%SZ")
        where = f'date_heure >= "{start}"'

        with make_session() as session:
            records = fetch_all(session, API_URL, where)

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = RAW_DIR / f"{data_interval_end:%Y-%m-%dT%H-%M}.json"
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(records, ensure_ascii=False))
        tmp_path.replace(path)

        print(f"{len(records)} lignes écrites dans {path}")
        return {"path": str(path), "start": start}

    @task
    def load(fetched: dict):
        records = json.loads(Path(fetched["path"]).read_text())
        rows = [
            (r["date_heure"], r["date"], r["heure"], json.dumps(r, ensure_ascii=False))
            for r in records
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.insert_rows(
            table=TABLE,
            rows=rows,
            target_fields=["date_heure", "date_locale", "heure_locale", "payload"],
            replace=True,
            replace_index=["date_heure", "date_locale", "heure_locale"],
            commit_every=len(rows),
        )
        print(f"{len(rows)} lignes chargées depuis {fetched['path']}")
        return {"rows": len(rows), "start": fetched["start"]}

    @task
    def check(loaded: dict):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        in_db = hook.get_first(
            f"SELECT count(*) FROM {TABLE} WHERE date_heure >= %s",
            parameters=(loaded["start"],),
        )[0]

        if in_db != loaded["rows"]:
            raise ValueError(f"Attendu {loaded['rows']} en base, trouvé {in_db}")
        print(f"OK : {in_db} lignes en base depuis {loaded['start']}")

    create = create_table()
    loaded = load(fetch())
    create >> loaded
    check(loaded)


eco2mix_realtime_ingestion()
