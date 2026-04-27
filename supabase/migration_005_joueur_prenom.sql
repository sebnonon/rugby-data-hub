-- Migration 005 — Ajout prénom dans la table joueur
ALTER TABLE joueur ADD COLUMN IF NOT EXISTS prenom TEXT;
