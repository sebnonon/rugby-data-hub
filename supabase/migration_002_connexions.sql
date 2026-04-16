-- Suivi des connexions au dashboard
CREATE TABLE IF NOT EXISTS connexion (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    nom         TEXT        NOT NULL,
    email       TEXT,
    session_id  TEXT        NOT NULL
);

-- Index pour requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_connexion_timestamp ON connexion (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_connexion_nom       ON connexion (nom);
