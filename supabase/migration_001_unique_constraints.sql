-- =============================================================
-- Migration 001 — Ajout des contraintes UNIQUE
-- À exécuter dans l'éditeur SQL Supabase
-- =============================================================
-- Contexte : permet l'import staff via delete+insert par _source_file
-- sans créer de doublons en cas de ré-upload d'un fichier.
--
-- IMPORTANT : vérifier l'absence de doublons avant d'exécuter.
-- Les requêtes de vérification sont fournies en commentaire.
-- =============================================================


-- -------------------------------------------------------------
-- VÉRIFICATIONS PRÉALABLES (à exécuter d'abord, résultat doit être 0)
-- -------------------------------------------------------------

-- SELECT COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text)       FROM perf_match;
-- SELECT COUNT(*) - COUNT(DISTINCT joueur_id::text || _source_file || titre) FROM perf_entrainement;
-- SELECT COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text || COALESCE(start_time,'') || COALESCE(mi_temps::text,'')) FROM collision;
-- SELECT COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text || COALESCE(mi_temps::text,'') || scrum_num::text) FROM melee;


-- -------------------------------------------------------------
-- 1. perf_match — UNIQUE(joueur_id, match_id)
-- -------------------------------------------------------------
ALTER TABLE perf_match
    ADD CONSTRAINT perf_match_joueur_match_unique
    UNIQUE (joueur_id, match_id);


-- -------------------------------------------------------------
-- 2. perf_entrainement — UNIQUE(joueur_id, _source_file, titre)
--    Rationale : un fichier = une session ; titre = drill dans la session.
--    Deux sessions le même jour peuvent avoir le même drill (ex: Activation).
-- -------------------------------------------------------------
ALTER TABLE perf_entrainement
    ADD CONSTRAINT perf_entr_joueur_source_titre_unique
    UNIQUE (joueur_id, _source_file, titre);


-- -------------------------------------------------------------
-- 3. collision — UNIQUE(joueur_id, match_id, start_time, mi_temps)
-- -------------------------------------------------------------
ALTER TABLE collision
    ADD CONSTRAINT collision_joueur_match_time_unique
    UNIQUE (joueur_id, match_id, start_time, mi_temps);


-- -------------------------------------------------------------
-- 4. melee — UNIQUE(joueur_id, match_id, mi_temps, scrum_num)
--    Rationale : scrum_num repart à 1 par fichier (= par mi-temps).
--    mi_temps distingue les deux halves.
-- -------------------------------------------------------------
ALTER TABLE melee
    ADD CONSTRAINT melee_joueur_match_mitemps_scrum_unique
    UNIQUE (joueur_id, match_id, mi_temps, scrum_num);
