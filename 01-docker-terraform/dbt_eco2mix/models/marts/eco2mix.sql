{{ config(materialized='table') }}

with unioned as (

    select *, 'consolide' as source, 1 as priority
    from {{ ref('stg_eco2mix') }}

    union all

    select *, 'temps_reel' as source, 2 as priority
    from {{ ref('stg_eco2mix_tr') }}

),

ranked as (

    select
        *,
        row_number() over (partition by date_heure order by priority) as rn
    from unioned

)

select
    date_heure,
    nature,
    source,
    consommation,
    nucleaire,
    eolien
from ranked
where rn = 1
