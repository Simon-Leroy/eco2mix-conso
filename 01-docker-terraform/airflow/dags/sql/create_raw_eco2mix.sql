CREATE TABLE IF NOT EXISTS raw_eco2mix (
    date_heure   timestamptz NOT NULL,
    date_locale  date        NOT NULL,
    heure_locale text        NOT NULL,
    payload      jsonb       NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (date_heure, date_locale, heure_locale)
);
