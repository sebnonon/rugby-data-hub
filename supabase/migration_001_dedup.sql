-- =============================================================
-- Déduplication préalable à migration_001_unique_constraints.sql
-- À exécuter dans l'éditeur SQL Supabase AVANT la migration.
-- Stratégie : conserver la ligne avec le plus grand id (la plus récente),
-- supprimer les autres doublons.
-- =============================================================


-- -------------------------------------------------------------
-- 1. perf_match — clé (joueur_id, match_id)
-- -------------------------------------------------------------
DELETE FROM perf_match
WHERE perf_match_id NOT IN (
    SELECT MAX(perf_match_id)
    FROM perf_match
    GROUP BY joueur_id, match_id
);


-- -------------------------------------------------------------
-- 2. perf_entrainement — clé (joueur_id, _source_file, titre)
-- -------------------------------------------------------------
DELETE FROM perf_entrainement
WHERE perf_entr_id NOT IN (
    SELECT MAX(perf_entr_id)
    FROM perf_entrainement
    GROUP BY joueur_id, _source_file, titre
);


-- -------------------------------------------------------------
-- 3. collision — clé (joueur_id, match_id, start_time, mi_temps)
-- -------------------------------------------------------------
DELETE FROM collision
WHERE collision_id NOT IN (
    SELECT MAX(collision_id)
    FROM collision
    GROUP BY joueur_id, match_id, start_time, mi_temps
);


-- -------------------------------------------------------------
-- 4. melee — clé (joueur_id, match_id, mi_temps, scrum_num)
-- -------------------------------------------------------------
DELETE FROM melee
WHERE melee_id NOT IN (
    SELECT MAX(melee_id)
    FROM melee
    GROUP BY joueur_id, match_id, mi_temps, scrum_num
);


-- -------------------------------------------------------------
-- Vérification finale (doit retourner 0 partout)
-- -------------------------------------------------------------
SELECT 'perf_match'       AS table_name, COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text) AS doublons FROM perf_match
UNION ALL
SELECT 'perf_entrainement', COUNT(*) - COUNT(DISTINCT joueur_id::text || _source_file || titre) FROM perf_entrainement
UNION ALL
SELECT 'collision',         COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text || COALESCE(start_time,'') || COALESCE(mi_temps::text,'')) FROM collision
UNION ALL
SELECT 'melee',             COUNT(*) - COUNT(DISTINCT joueur_id::text || match_id::text || COALESCE(mi_temps::text,'') || scrum_num::text) FROM melee;
