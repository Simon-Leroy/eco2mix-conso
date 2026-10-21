import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.sdk import dag, task

API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
API_PARAMS = {
	"limit": 20,
	"refine": 'date_heure:2026'
}


def make_session():
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
	schedule=None,
	start_date=datetime(2026, 1, 1),
	catchup=False,
	tags=["demo"],
)
def eco2mix_ingestion():

	@task
	def fetch():
		response = requests.get(API_URL, params=API_PARAMS, timeout=10)
		response.raise_for_status()
		return response.json()

	@task
	def show_data(data: dict):
		print(data)

	show_data(fetch())


eco2mix_ingestion()
