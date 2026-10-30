#!/usr/bin/env bash
# Démarre la pile Docker et lance le chargement initial des données.
# Le script peut être relancé sans risque : les chargements sont idempotents (upsert),
# et le backfill ne recrée que les runs manquants ou en échec.
set -euo pipefail

cd "$(dirname "$0")/.."

# --- Réglages -----------------------------------------------------------------------
BACKFILL_FROM="2026-01-01"      # début du consolidé
BACKFILL_TO="2026-09-01"        # le consolidé s'arrête fin août 2026
REALTIME_LOOKBACK_DAYS=60       # chargement initial du temps réel (2 jours ensuite)
DAGS=(eco2mix_ingestion eco2mix_realtime_ingestion dbt_build)

# Exécute une commande Airflow dans le conteneur scheduler
airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }

# --- 1. Fichier .env ----------------------------------------------------------------
if [ ! -f .env ]; then
  echo "Création de .env à partir de .env.example"
  cp .env.example .env
  # UID de ton utilisateur : les fichiers écrits par Airflow t'appartiennent
  sed -i "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" .env
  # Clé Fernet : 32 octets aléatoires en base64 "url-safe" (+ -> -, / -> _)
  sed -i "s|^FERNET_KEY=.*|FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')|" .env
fi
if ! grep -q '^FERNET_KEY=.\+' .env; then
  echo "FERNET_KEY est vide dans .env : renseigne-la, puis relance." >&2
  exit 1
fi

# --- 2. Démarrage des conteneurs ----------------------------------------------------
echo "Démarrage des conteneurs..."
docker compose up -d

# --- 3. Activation des DAGs ---------------------------------------------------------
# Ils sont en pause à la création. L'activation échoue tant que le dag-processor ne les
# a pas analysés : on réessaie pendant 5 minutes au maximum.
echo "Activation des DAGs..."
for dag in "${DAGS[@]}"; do
  for attempt in $(seq 1 60); do
    if airflow dags unpause "$dag" >/dev/null 2>&1; then
      echo "  actif : $dag"
      break
    fi
    if [ "$attempt" -eq 60 ]; then
      echo "Délai dépassé : le DAG $dag est introuvable." >&2
      exit 1
    fi
    sleep 5
  done
done

# --- 4. Chargement initial ----------------------------------------------------------
echo "Backfill du consolidé ($BACKFILL_FROM -> $BACKFILL_TO)..."
# "failed" = créer les runs manquants ET relancer ceux en échec, sans refaire les réussis
if ! airflow backfill create --dag-id eco2mix_ingestion \
      --from-date "$BACKFILL_FROM" --to-date "$BACKFILL_TO" \
      --reprocess-behavior failed; then
  echo "  (backfill non lancé : un backfill est peut-être déjà en cours)"
fi

echo "Chargement initial du temps réel ($REALTIME_LOOKBACK_DAYS jours)..."
airflow dags trigger eco2mix_realtime_ingestion \
  --conf "{\"lookback_days\": $REALTIME_LOOKBACK_DAYS}"

# dbt_build n'est pas lancé ici : il se déclenche tout seul après chaque chargement
# (Assets raw_eco2mix et raw_eco2mix_tr).

cat <<'EOF'

Terminé. Le chargement tourne en arrière-plan :
  Airflow : http://localhost:8080   (airflow / airflow)
  pgAdmin : http://localhost:8085   (admin@admin.com / root, hôte "postgres")
EOF
