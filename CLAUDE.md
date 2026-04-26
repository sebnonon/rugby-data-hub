# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Rugby Data Hub is an ETL pipeline + Streamlit dashboard for analyzing GPS data from STATSports sensors. Raw CSV exports from STATSports are parsed, stored in Supabase (PostgreSQL), and visualized through a multi-page Streamlit app.

**Live demo:** https://rugby-data-hub.fly.dev/

## Commands

```bash
# Set up
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run the dashboard locally
streamlit run dashboard/app.py

# Run the full local ETL pipeline (CSV → SQLite)
python pipeline/pipeline.py

# Migrate SQLite → Supabase
python pipeline/migrate_to_supabase.py

# Generate demo data (requires .env.demo)
python pipeline/generate_demo_data.py

# Explore local SQLite data ad-hoc
python pipeline/explorer.py
```

There are no automated tests.

## Environment variables

Create `.env` at the project root for local development:
```
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
```

For the demo data generator, use `.env.demo` with `SUPABASE_URL_DEMO` and `SUPABASE_SERVICE_ROLE_KEY_DEMO`.

In production (Fly.io), credentials are set via `fly secrets set`. `pipeline/supabase_client.py` handles both environments automatically, loading from Streamlit Secrets first, then falling back to `.env`.

## Architecture

### Data flow

```
data/raw/gps_match/*.csv
data/raw/gps_entrainement/*.csv   →  pipeline/parsers/  →  pipeline.py  →  SQLite (dev)
data/raw/gps_collision/*.csv                                              →  Supabase (prod)
data/raw/gps_melee/*.csv
data/raw/actions_match/*.csv

Staff upload  →  dashboard/pages/2_Import.py  →  parsers/  →  Supabase directly
```

### Pipeline parsers (`pipeline/parsers/`)

Each parser handles one CSV type and returns a cleaned DataFrame ready for upsert:

| File | Source → Table |
|---|---|
| `gps_match.py` | GPS match CSVs → `perf_match` |
| `gps_entrainement.py` | GPS training CSVs → `perf_entrainement` |
| `gps_collision.py` | Collision CSVs → `collision` |
| `gps_melee.py` | Scrum CSVs → `melee` |
| `actions_match.py` | Video coding CSVs → `perf_match` + `touche` + `match` (including `score_rec`, `score_adv`, `adversaire_nom_complet`) |
| `utils.py` | Shared helpers (normalization, FK resolution) |

`utils.py` is central — it contains `HOME_TEAM`, team/player aliases, `assign_fk()` (joins player/match reference DataFrames), and all normalization functions (`normalize_match_name`, `normalize_date`, `normalize_session_type`, etc.). `ABBREV_TO_FULL_NAME` is the inverse of `TEAM_FULL_NAMES` (abbreviation → full name) and is derived automatically — no separate maintenance needed.

### `perf_match` dual-source upsert

`perf_match` is populated by two independent parsers that upsert into the same row via the `(joueur_id, match_id)` unique constraint:
- `gps_match.py` fills the GPS columns (`total_distance`, `max_speed`, `sprints`, etc.)
- `actions_match.py` fills the action columns (`passes_total`, `plaquages_total`, `minutes_jouees`, etc.)

A `perf_match` row can therefore have GPS data only, actions data only, or both. Always handle NULLs in both column groups when querying.

### Database schema (Supabase/PostgreSQL)

Reference tables: `joueur`, `match`  
Fact tables: `perf_match`, `perf_entrainement`, `collision`, `melee`, `touche`  
Views: `view_match_total`, `view_charge_hebdo`, `view_collisions_par_match`

Key fields:
- `joueur.poste_principal` — only player metadata beyond name; used for position-based filtering in the dashboard
- `perf_match.minutes_jouees` — playing time per match, essential for normalizing per-minute stats
- `perf_entrainement.acute_chronic_ratio` — ACWR injury-risk indicator
- `match.score_rec` / `match.score_adv` — match scores computed from video coding actions (essai=5, transfo=2, pénalité=3, drop=3)
- `match.adversaire_nom_complet` — full opponent name derived from `ABBREV_TO_FULL_NAME`

Full schema: `supabase/schema.sql`

### Dashboard (`dashboard/`)

Entry point: `dashboard/app.py` — uses `st.navigation()` for multi-page routing. **New pages must be registered there.**

Pages numbered by load order (not navigation index):
- `1_Performances.py` — GPS metrics per player per match
- `2_Import.py` — Staff CSV upload with password (`STAFF_PASSWORD` secret), validation, and direct Supabase import
- `3_Entrainements.py` — Training load, ACWR
- `4_Collisions.py` — Collision events
- `5_Melees.py` — Scrum data
- `6_Explorer.py` — Top-N rankings, multi-player comparison, radar charts

## CSV filename conventions (required for dashboard upload)

| Type | Format |
|---|---|
| GPS Match | `gps_match_EQ1-EQ2_YYYY-MM-DD.csv` |
| GPS Training | `gps_entrainement_YYYY-MM-DD_(Reprise\|J-2\|J-1\|J+2).csv` |
| GPS Collision | `gps_collision_EQ1-EQ2_YYYY-MM-DD.csv` |
| GPS Scrum | `gps_melee_EQ1-EQ2_YYYY-MM-DD.csv` |
| Actions | `actions_match_EQ1-EQ2_YYYY-MM-DD.csv` |

The local pipeline (`pipeline.py`) does not enforce these conventions.

## Club configuration

`pipeline/parsers/utils.py` line 11: `HOME_TEAM = "REC"` — change this to adapt the project for a different club. Also update `MATCH_ALIASES` and `TEAM_FULL_NAMES` in the same file.

**Warning:** `MATCH_ALIASES` and `TEAM_FULL_NAMES` are duplicated verbatim in `pipeline/pipeline.py`. Both files must be updated together when changing clubs.

## Data

`data/raw/` is gitignored — CSV files are confidential STATSports exports. The demo uses synthetic data generated by `pipeline/generate_demo_data.py`.
