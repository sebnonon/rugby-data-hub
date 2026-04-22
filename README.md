# Rugby Data Hub — Plateforme Data GPS

> Pipeline ETL + dashboard d'analyse GPS et codage vidéo pour clubs de rugby.  
> Transforme les exports bruts STATSports et les fichiers de codage vidéo en tableau de bord interactif accessible au staff technique.

En travaillant avec un club de rugby professionnel, j'ai réalisé que les capteurs GPS STATSports généraient chaque semaine des dizaines de fichiers CSV. Il y a là une réelle difficulté à centraliser les données et à retrouver des informations dans tous ces fichiers Excel, faute d'outil adapté. Ce projet est né de cette frustration : construire de A à Z un pipeline de données et un dashboard utilisable directement par le staff. Le but n'est pas de réinventer le travail des membres du staff, mais de le faciliter.

[![Demo](https://img.shields.io/badge/Demo-Live-00C851?logo=fly.io&logoColor=white)](https://rugby-data-hub.fly.dev/)
[![Fly.io](https://img.shields.io/badge/Fly.io-deployed-7C3AED?logo=flydotio&logoColor=white)](https://fly.io)
[![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-3ECF8E?logo=supabase&logoColor=white)](https://supabase.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)

**[👉 Voir la démo en ligne](https://rugby-data-hub.fly.dev/)**

---

## Présentation

Les clubs de rugby équipés de capteurs GPS STATSports exportent chaque semaine des fichiers CSV par joueur — distance, vitesse, sprints, collisions, mêlées. En parallèle, les analystes vidéo produisent des fichiers de codage match (passes, plaquages, franchissements, présences en touche et mêlée). Ces deux sources de données existent mais restent cloisonnées, faute d'outil pour les croiser et les visualiser ensemble.

Ce projet construit un pipeline complet de bout en bout :

```
CSV STATSports (GPS)  ─┐
CSV codage vidéo      ─┴→ parsing Python → PostgreSQL (Supabase) → dashboard Streamlit
```

Le croisement GPS + vidéo permet par exemple de mettre en regard la charge physique d'un joueur avec son volume d'actions techniques sur le même match. Le staff peut importer ses fichiers directement depuis le dashboard, sans ligne de code.

---

## Dashboard — aperçu

**[👉 Voir la démo en ligne](https://rugby-data-hub.fly.dev/)**

**6 pages disponibles :**

| Page | Contenu |
|---|---|
| Performances | Distance, vitesse max, sprints, HSR + actions vidéo (passes, plaquages…) par joueur et par match |
| Entraînements | Charge par séance (J-2 / J-1 / J+2 / Reprise), ACWR hebdomadaire |
| Collisions | Nombre de contacts, charge, distribution d'intensité, time to feet |
| Mêlées | Scrum load, impact individuel des avants, timeline par match |
| Explorateur | Classement top N, comparaison multi-joueurs, radar métriques |
| Import | Upload CSV, validation et aperçu des données (démo) |

---

## Architecture

### Flux de données

```
data/raw/gps_match/*.csv          ─┐
data/raw/gps_entrainement/*.csv    ├─→ pipeline/parsers/ → pipeline.py → SQLite (dev)
data/raw/gps_collision/*.csv       │                                   → Supabase (prod)
data/raw/gps_melee/*.csv           │
data/raw/actions_match/*.csv      ─┘

Upload staff → dashboard/pages/2_Import.py → parsers/ → Supabase directement
```

### Stack technique

| Couche | Technologie |
|---|---|
| Sources | CSV STATSports (GPS match, entraînement, collision, mêlée) + exports codage vidéo (actions individuelles par match) |
| Pipeline ETL | Python 3.12 · pandas |
| Base de développement | SQLite (local) |
| Base de production | Supabase (PostgreSQL) |
| Dashboard | Streamlit · Plotly |
| Déploiement | Fly.io (Docker) |

### Schéma de données

**Tables de référence** : `joueur` · `match`

**Tables de faits** :
- `perf_match` — métriques GPS + actions par joueur par match
- `perf_entrainement` — métriques GPS par joueur par session
- `collision` — événements de collision individuels
- `melee` — données mêlées par joueur et par mêlée
- `touche` — données de touche par match

**Vues** : `view_match_total` · `view_charge_hebdo` · `view_collisions_par_match`

Schéma complet : [`supabase/schema.sql`](supabase/schema.sql)

### Structure du projet

```
pipeline/
  parsers/
    utils.py              ← helpers partagés (normalisation, résolution FK)
    gps_match.py          ← parser GPS matchs → perf_match
    gps_entrainement.py   ← parser GPS entraînements → perf_entrainement
    gps_collision.py      ← parser collisions → collision
    gps_melee.py          ← parser mêlées → melee
    actions_match.py      ← parser codage vidéo → perf_match + touche + match
  pipeline_rec.py         ← ETL complet CSV → SQLite
  migrate_to_supabase.py  ← migration SQLite → Supabase
  supabase_client.py      ← client Supabase (dual-env .env / Streamlit Secrets)

dashboard/
  app.py                  ← point d'entrée multi-page (st.navigation)
  pages/
    1_Performances.py
    2_Import.py
    3_Entrainements.py
    4_Collisions.py
    5_Melees.py
    6_Explorer.py

supabase/
  schema.sql
```

---

## Démarrage rapide

```bash
# 1. Créer l'environnement
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Configurer les credentials Supabase
cp .env.example .env
# → renseigner SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY

# 3. Déposer les CSV STATSports dans data/raw/

# 4. Lancer le pipeline ETL local
python pipeline/pipeline_rec.py

# 5. Migrer vers Supabase
python pipeline/migrate_to_supabase.py

# 6. Lancer le dashboard
streamlit run dashboard/app.py
```

---

## Nomenclature des fichiers CSV

Obligatoire pour l'upload via le dashboard :

| Type | Format attendu |
|---|---|
| GPS Match | `gps_match_EQ1-EQ2_YYYY-MM-DD.csv` |
| GPS Entraînement | `gps_entrainement_YYYY-MM-DD_(Reprise\|J-2\|J-1\|J+2).csv` |
| GPS Collision | `gps_collision_EQ1-EQ2_YYYY-MM-DD.csv` |
| GPS Mêlée | `gps_melee_EQ1-EQ2_YYYY-MM-DD.csv` |
| Actions Match | `actions_match_EQ1-EQ2_YYYY-MM-DD.csv` |

```
gps_match_REC-OMR_2025-11-15.csv
gps_entrainement_2025-11-14_J-2.csv
gps_collision_REC-NRC_2025-09-13.csv
```

Le pipeline local (`pipeline_rec.py`) n'impose pas cette nomenclature.

---

## Variables d'environnement

| Variable | Contexte | Description |
|---|---|---|
| `SUPABASE_URL` | `.env` / Fly secrets | URL du projet Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | `.env` / Fly secrets | Clé service role Supabase |

En développement local : fichier `.env` à la racine (voir `.env.example`).  
En production : secrets Fly.io (`fly secrets set`).

---

## Données

Les fichiers CSV sources sont des exports STATSports confidentiels appartenant au club — ils ne sont pas versionnés (`data/raw/` est dans `.gitignore`).

---

## Ce que j'ai appris

- Construire un pipeline ETL complet en Python (parsing, normalisation, résolution de clés étrangères)
- Modélisation d'une base de données relationnelle PostgreSQL (Supabase) pour des données sportives
- Développement d'une app multi-page avec Streamlit et Plotly
- Conteneurisation Docker et déploiement continu sur Fly.io via GitHub Actions
- Génération de données synthétiques réalistes pour valider un pipeline sans exposer de données réelles

---

## Auteur

**Sébastien Nonon**  
[linkedin.com/in/sébastien-nonon-5b9645298](https://www.linkedin.com/in/sébastien-nonon-5b9645298) · nonon.sebastien@gmail.com
