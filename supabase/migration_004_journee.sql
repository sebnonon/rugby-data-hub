-- Migration 004 — Ajout journée de championnat dans la table match
ALTER TABLE match ADD COLUMN IF NOT EXISTS journee INTEGER;
