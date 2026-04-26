-- =============================================================
-- Schéma PostgreSQL — Rugby Data Hub (Rugby Data Hub)
-- Migration SQLite → Supabase
-- =============================================================

-- ------------------------------------------------------------
-- Table de référence : joueur
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS joueur (
    joueur_id   SERIAL PRIMARY KEY,
    nom         TEXT NOT NULL,
    poste_principal TEXT
);

-- ------------------------------------------------------------
-- Table de référence : match
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match (
    match_id                INTEGER PRIMARY KEY,
    session_title           TEXT,
    date                    DATE,
    equipe_dom              TEXT,
    equipe_ext              TEXT,
    adversaire              TEXT,
    adversaire_nom_complet  TEXT,
    score_rec               INTEGER,
    score_adv               INTEGER,
    competition             TEXT,
    melee_total_rec         INTEGER,
    melee_positif_rec       INTEGER,
    melee_negatif_rec       INTEGER,
    melee_neutre_rec        INTEGER,
    melee_total_adv         INTEGER,
    melee_positif_adv       INTEGER,
    melee_negatif_adv       INTEGER,
    melee_neutre_adv        INTEGER,
    ruck_rec                INTEGER,
    ruck_adv                INTEGER,
    lancement_touche_rec    INTEGER,
    lancement_touche_adv    INTEGER,
    lancement_melee_rec     INTEGER,
    lancement_melee_adv     INTEGER,
    penalite_rec            INTEGER,
    penalite_adv            INTEGER,
    turnover_rec            INTEGER,
    turnover_adv            INTEGER,
    possession_rec          INTEGER,
    possession_adv          INTEGER,
    ballon_perdu_rec        INTEGER,
    ballon_perdu_adv        INTEGER,
    sequence_jeu_adv        INTEGER
);

-- ------------------------------------------------------------
-- Table de faits : perf_match
-- GPS + actions par joueur, par match
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS perf_match (
    perf_match_id                   SERIAL PRIMARY KEY,
    joueur_id                       INTEGER REFERENCES joueur(joueur_id),
    match_id                        INTEGER REFERENCES match(match_id),
    UNIQUE (joueur_id, match_id),
    -- GPS
    total_time                      TEXT,
    average_speed                   REAL,
    total_distance                  REAL,
    distance_per_min                REAL,
    sprints                         REAL,
    sprint_distance                 REAL,
    max_speed                       REAL,
    high_speed_running_absolute     REAL,
    hsr_per_minute_absolute         REAL,
    hml_distance                    REAL,
    hmld_per_minute                 REAL,
    accelerations_absolute          REAL,
    decelerations_absolute          REAL,
    accelerations_per_min_absolute  REAL,
    decels_per_min_absolute         REAL,
    step_balance                    REAL,
    collision_load                  REAL,
    collisions                      REAL,
    dynamic_stress_load             REAL,
    metabolic_distance_absolute     REAL,
    average_metabolic_power         REAL,
    max_acceleration                REAL,
    max_deceleration                REAL,
    acute                           REAL,
    chronic                         REAL,
    acute_chronic_ratio             REAL,
    _source_file_gps                TEXT,
    -- Actions
    passes_total                    INTEGER,
    passes_positif                  INTEGER,
    passes_negatif                  INTEGER,
    passes_neutre                   INTEGER,
    porteur_total                   INTEGER,
    porteur_positif                 INTEGER,
    porteur_negatif                 INTEGER,
    porteur_neutre                  INTEGER,
    plaquages_total                 INTEGER,
    plaquages_positif               INTEGER,
    plaquages_negatif               INTEGER,
    plaquages_neutre                INTEGER,
    soutiens_total                  INTEGER,
    soutiens_positif                INTEGER,
    soutiens_negatif                INTEGER,
    soutiens_neutre                 INTEGER,
    contacts_total                  INTEGER,
    contacts_positif                INTEGER,
    contacts_negatif                INTEGER,
    contacts_neutre                 INTEGER,
    ballons_perdus_total            INTEGER,
    ballons_perdus_positif          INTEGER,
    ballons_perdus_negatif          INTEGER,
    ballons_perdus_neutre           INTEGER,
    fautes_total                    INTEGER,
    fautes_positif                  INTEGER,
    fautes_negatif                  INTEGER,
    fautes_neutre                   INTEGER,
    def_battus_total                INTEGER,
    def_battus_positif              INTEGER,
    def_battus_negatif              INTEGER,
    def_battus_neutre               INTEGER,
    duels_aeriens_total             INTEGER,
    duels_aeriens_positif           INTEGER,
    duels_aeriens_negatif           INTEGER,
    duels_aeriens_neutre            INTEGER,
    franchissements_total           INTEGER,
    franchissements_positif         INTEGER,
    franchissements_negatif         INTEGER,
    franchissements_neutre          INTEGER,
    interceptions_total             INTEGER,
    interceptions_positif           INTEGER,
    interceptions_negatif           INTEGER,
    interceptions_neutre            INTEGER,
    essais_total                    INTEGER,
    essais_positif                  INTEGER,
    essais_negatif                  INTEGER,
    essais_neutre                   INTEGER,
    passes_contact_total            INTEGER,
    passes_contact_positif          INTEGER,
    passes_contact_negatif          INTEGER,
    passes_contact_neutre           INTEGER,
    grattages_total                 INTEGER,
    grattages_positif               INTEGER,
    grattages_negatif               INTEGER,
    grattages_neutre                INTEGER,
    contests_total                  INTEGER,
    contests_positif                INTEGER,
    contests_negatif                INTEGER,
    contests_neutre                 INTEGER,
    contre_rucks_total              INTEGER,
    contre_rucks_positif            INTEGER,
    contre_rucks_negatif            INTEGER,
    contre_rucks_neutre             INTEGER,
    jap_total                       INTEGER,
    jap_positif                     INTEGER,
    jap_negatif                     INTEGER,
    jap_neutre                      INTEGER,
    jap_depuis_propre_22m           INTEGER,
    jap_depuis_mi_terrain           INTEGER,
    jap_depuis_camp_adverse         INTEGER,
    jap_en_touche                   INTEGER,
    jap_camp_adverse                INTEGER,
    minutes_jouees                  INTEGER,
    buts_penalite_reussis           INTEGER,
    buts_penalite_rates             INTEGER,
    buts_transfo_reussies           INTEGER,
    buts_transfo_ratees             INTEGER,
    buts_drop_reussis               INTEGER,
    buts_drop_rates                 INTEGER,
    cartons_jaunes                  INTEGER,
    cartons_rouges                  INTEGER,
    _source_file_actions            TEXT
);

-- ------------------------------------------------------------
-- Table de faits : perf_entrainement
-- GPS par joueur, par séance d'entraînement
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS perf_entrainement (
    perf_entr_id                    SERIAL PRIMARY KEY,
    joueur_id                       INTEGER REFERENCES joueur(joueur_id),
    UNIQUE (joueur_id, _source_file, titre),
    seance_type                     TEXT,
    session_type                    TEXT,
    titre                           TEXT,
    date                            DATE,
    total_time                      TEXT,
    total_distance                  REAL,
    distance_per_min                REAL,
    max_speed                       REAL,
    sprints                         INTEGER,
    sprint_distance                 REAL,
    high_speed_running_absolute     REAL,
    hsr_per_minute_absolute         REAL,
    hml_distance                    REAL,
    hmld_per_minute                 REAL,
    accelerations_absolute          INTEGER,
    decelerations_absolute          INTEGER,
    dynamic_stress_load             REAL,
    metabolic_distance_absolute     REAL,
    max_acceleration                REAL,
    acute                           INTEGER,
    chronic                         INTEGER,
    acute_chronic_ratio             INTEGER,
    _source_file                    TEXT
);

-- ------------------------------------------------------------
-- Table de faits : collision
-- Événements de collision individuels par joueur, par match
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collision (
    collision_id            SERIAL PRIMARY KEY,
    joueur_id               INTEGER REFERENCES joueur(joueur_id),
    match_id                INTEGER REFERENCES match(match_id),
    UNIQUE (joueur_id, match_id, start_time, mi_temps),
    mi_temps                INTEGER,
    start_time              TEXT,
    end_time                TEXT,
    duration                TEXT,
    collision_load          REAL,
    time_to_feet            REAL,
    post_collision_accel    REAL,
    _source_file            TEXT
);

-- ------------------------------------------------------------
-- Table de faits : melee
-- Données de mêlée par joueur, par mêlée
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS melee (
    melee_id                SERIAL PRIMARY KEY,
    joueur_id               INTEGER REFERENCES joueur(joueur_id),
    match_id                INTEGER REFERENCES match(match_id),
    UNIQUE (joueur_id, match_id, mi_temps, scrum_num),
    scrum_id                INTEGER,
    scrum_num               INTEGER,
    mi_temps                INTEGER,
    start_time              TEXT,
    end_time                TEXT,
    duration                TEXT,
    avg_total_impact        TEXT,
    avg_front_row_impact    TEXT,
    avg_second_row_impact   TEXT,
    avg_back_row_impact     TEXT,
    scrum_sync_time         TEXT,
    impact                  TEXT,
    sync_time               TEXT,
    scrum_load              TEXT,
    time_to_feet            TEXT,
    post_scrum_accel        TEXT,
    _source_file            TEXT
);

-- ------------------------------------------------------------
-- Table de faits : touche
-- Données de touche par match
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS touche (
    touche_id   SERIAL PRIMARY KEY,
    match_id    INTEGER REFERENCES match(match_id),
    equipe      TEXT,
    est_rec     INTEGER,
    resultat    TEXT,
    alignement  TEXT,
    sortie      TEXT,
    zone        TEXT,
    start_sec   REAL,
    _source_file TEXT
);

-- ------------------------------------------------------------
-- Vue : view_collisions_par_match
-- Métriques de collision agrégées par joueur et par match
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW view_collisions_par_match AS
SELECT
    c.joueur_id,
    j.nom                       AS joueur,
    c.match_id,
    m.session_title             AS match_titre,
    COUNT(*)                    AS nb_collisions,
    AVG(c.collision_load)       AS moy_collision_load,
    MAX(c.collision_load)       AS max_collision_load,
    AVG(c.time_to_feet)         AS moy_time_to_feet,
    SUM(c.post_collision_accel) AS total_post_accels
FROM collision c
LEFT JOIN joueur j ON c.joueur_id = j.joueur_id
LEFT JOIN match  m ON c.match_id  = m.match_id
GROUP BY c.joueur_id, j.nom, c.match_id, m.session_title;

-- ------------------------------------------------------------
-- Vue : view_charge_hebdo
-- Charge hebdomadaire par joueur (base ACWR)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW view_charge_hebdo AS
SELECT
    joueur_id,
    TO_CHAR(DATE_TRUNC('week', date), 'IYYY-IW') AS semaine,
    session_type,
    COUNT(*)                            AS nb_sessions,
    SUM(total_distance)                 AS total_distance,
    SUM(accelerations_absolute)         AS total_accels,
    SUM(dynamic_stress_load)            AS total_dsl
FROM perf_entrainement
WHERE date IS NOT NULL
GROUP BY joueur_id, DATE_TRUNC('week', date), session_type;

-- ------------------------------------------------------------
-- Vue : view_match_total
-- Agrège les 2 mi-temps par joueur/match (depuis perf_match)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW view_match_total AS
SELECT
    joueur_id,
    match_id,
    SUM(total_distance)                 AS total_distance,
    SUM(sprints)                        AS total_sprints,
    SUM(sprint_distance)                AS total_sprint_distance,
    MAX(max_speed)                      AS max_speed,
    SUM(high_speed_running_absolute)    AS total_hsr,
    SUM(hml_distance)                   AS total_hml,
    SUM(accelerations_absolute)         AS total_accels,
    SUM(decelerations_absolute)         AS total_decels,
    SUM(dynamic_stress_load)            AS total_dsl,
    SUM(metabolic_distance_absolute)    AS total_metabolic_distance,
    SUM(collisions)                     AS total_collisions,
    SUM(collision_load)                 AS total_collision_load
FROM perf_match
GROUP BY joueur_id, match_id;
