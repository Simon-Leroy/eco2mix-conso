-- Verrou contre la création simultanée de la table par plusieurs runs
-- (voir create_raw_eco2mix.sql pour le détail).
SELECT pg_advisory_xact_lock(hashtext('create_raw_eco2mix_tr'));

CREATE TABLE IF NOT EXISTS raw_eco2mix_tr (
    date_heure   timestamptz NOT NULL,
    date_locale  date        NOT NULL,
    heure_locale text        NOT NULL,
    payload      jsonb       NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (date_heure, date_locale, heure_locale)
);
