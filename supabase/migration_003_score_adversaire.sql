-- Migration 003 — Ajout score et nom complet adversaire dans la table match
ALTER TABLE match ADD COLUMN IF NOT EXISTS adversaire_nom_complet TEXT;
ALTER TABLE match ADD COLUMN IF NOT EXISTS score_rec INTEGER;
ALTER TABLE match ADD COLUMN IF NOT EXISTS score_adv INTEGER;
