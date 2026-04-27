-- Backfill journee sur les données démo existantes (journee = match_id)
UPDATE match SET journee = match_id WHERE journee IS NULL;
