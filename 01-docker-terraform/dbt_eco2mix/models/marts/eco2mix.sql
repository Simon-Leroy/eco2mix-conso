{{ config(materialized='table') }}

with unioned as (

    -- Le consolidé n'a pas les 4 dernières colonnes : on les crée à null,
    -- dans le même ordre que stg_eco2mix_tr, pour pouvoir empiler les deux.
    select
        *,
        null::numeric as eolien_terrestre,
        null::numeric as eolien_offshore,
        null::numeric as stockage_batterie,
        null::numeric as destockage_batterie,
        'consolide' as source
    from {{ ref('stg_eco2mix') }}

    union all

    select
        *,
        'temps_reel' as source
    from {{ ref('stg_eco2mix_tr') }}

)

-- distinct on garde une seule ligne par créneau : la première selon le order by.
-- (source = 'temps_reel') vaut false pour le consolidé, true pour le temps réel,
-- et false est trié avant true : le consolidé gagne quand il existe.
select distinct on (date_heure) *
from unioned
order by date_heure, (source = 'temps_reel')
