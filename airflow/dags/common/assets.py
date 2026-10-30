from airflow.sdk import Asset

RAW_ECO2MIX = Asset(
    name="raw_eco2mix",
    uri="postgres://postgres:5432/eco2mix_national/public/raw_eco2mix"
)
RAW_ECO2MIX_TR = Asset(
    name="raw_eco2mix_tr",
    uri="postgres://postgres:5432/eco2mix_national/public/raw_eco2mix_tr"
)
