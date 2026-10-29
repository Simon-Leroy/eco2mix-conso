-- Plusieurs runs peuvent démarrer en même temps (backfill) : sans verrou, deux sessions
-- voient "la table n'existe pas" et la créent en même temps, l'une échoue
-- (UniqueViolation sur pg_type). Le verrou fait attendre la seconde session ;
-- il est relâché automatiquement à la fin de la transaction.
SELECT pg_advisory_xact_lock(hashtext('create_raw_eco2mix'));

CREATE TABLE IF NOT EXISTS raw_eco2mix (
    date_heure   timestamptz NOT NULL,
    date_locale  date        NOT NULL,
    heure_locale text        NOT NULL,
    payload      jsonb       NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (date_heure, date_locale, heure_locale)
);
