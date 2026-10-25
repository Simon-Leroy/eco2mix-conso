select
    date_heure,
    payload->>'nature' as nature,
    (payload->>'consommation')::numeric as consommation,
    (payload->>'nucleaire')::numeric as nucleaire,
    (payload->>'eolien')::numeric as eolien
from {{ source('raw', 'raw_eco2mix') }}
