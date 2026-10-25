import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PAGE_SIZE = 100


def make_session():
    """Crée une session HTTP qui réessaie automatiquement en cas d'erreur temporaire.

    Une Session réutilise la connexion entre les appels (plus rapide que requests.get)
    et le HTTPAdapter y attache la logique de retry, ici pour toutes les URL en https.
    """
    retry = Retry(
        total=3,
        backoff_factor=1,  # délai de retry -> backoff * 2^(n - 1)
        status_forcelist=[429, 500, 502, 503, 504],  # codes sur lesquels on fait un retry
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(session, api_url, where):
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
        response = session.get(api_url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        records.extend(payload["results"])
        offset += PAGE_SIZE
        if offset >= payload["total_count"]:
            break
    return records
