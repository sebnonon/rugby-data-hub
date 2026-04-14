"""
Pipeline d'ingestion Rugby Data Hub — dev local (SQLite)
Sources : data/raw/gps_match/*.csv        — exports STATSports GPS match
          data/raw/gps_entrainement/*.csv  — exports STATSports GPS entraînement
          data/raw/gps_melee/*.csv         — exports STATSports GPS mêlée (format spécifique)
          data/raw/gps_collision/*.csv     — exports STATSports collision match
          data/raw/actions_match/*.csv     — fichiers de codage vidéo
"""

import sys
import pandas as pd
import sqlite3
import re
from pathlib import Path
from datetime import datetime

# Ajout du dossier pipeline au path pour l'import des parsers
sys.path.insert(0, str(Path(__file__).parent))
from parsers import gps_match as p_gps_matchs
from parsers import gps_entrainement as p_gps_entr
from parsers import gps_collision as p_gps_col
from parsers import gps_melee as p_gps_mel
from parsers import actions_match as p_actions
from parsers.utils import (
    normalize_match_name, normalize_date, snake, normalize_actions_match,
    MATCH_ALIASES, PLAYER_ALIASES, TEAM_FULL_NAMES, ACTIONS_STANDARD,
    zone_start_cat, zone_end_cat, parse_teams,
)

# ── Config ────────────────────────────────────────────────────────────────────
GPS_MATCHS_DIR      = Path("/home/nonon/rugby-data-hub/data/raw/gps_match")
GPS_ENTR_DIR        = Path("/home/nonon/rugby-data-hub/data/raw/gps_entrainement")
GPS_MELEES_DIR      = Path("/home/nonon/rugby-data-hub/data/raw/gps_melee")
GPS_COLLISIONS_DIR  = Path("/home/nonon/rugby-data-hub/data/raw/gps_collision")
ACTIONS_DIR         = Path("/home/nonon/rugby-data-hub/data/raw/actions_match")
DB_PATH             = Path("/home/nonon/rugby-data-hub/data/db/rugby_data.db")

# Aliases pour les cas où l'ordre des équipes diffère entre fichiers
MATCH_ALIASES = {
    "RNR-REC": "REC-RNR",
    "OMR-REC": "REC-OMR",
    "NRC-REC": "REC-NRC",
}

# Mapping noms complets → abréviations (utilisé pour les fichiers de codage actions)
TEAM_FULL_NAMES = {
    "Rennes Etudiants Club":          "REC",
    "Ol Marcquois Rugby":             "OMR",
    "CS Bourgoin Jallieu":            "CSBJ",
    "Niort Rugby Club":               "NRC",
    "RC Narbonne":                    "RCN",
    "SC Albi":                        "SCA",
    "Stado Tarbes Pyrenees Rugby":    "STPR",
    "Stade Niçois":                   "NR",
    "Rouen Normandie":                "RNR",
    "CA Perigourdin":                 "CAP",
    "RC Massy Essonne":               "RCME",
    "SO Chambery Rugby":              "SOC",
    "RC Suresnes Hauts De Seine":     "RCS",
    "US Bressane":                    "USBPA",
}


# Aliases pour les noms de joueurs qui diffèrent entre fichiers sources
PLAYER_ALIASES = {
    "TUUGAHALA O": "TUUGAHALA",
}

# ── Mapping actions codage vidéo → préfixes de colonnes perf_match ───────────
# Actions standard : comptage par résultat (total / positif / negatif / neutre)
ACTIONS_STANDARD = {
    "Passe":              "passes",
    "Porteur":            "porteur",
    "Plaquage":           "plaquages",
    "Soutien offensif":   "soutiens",
    "Contact":            "contacts",
    "Ballon perdu":       "ballons_perdus",
    "Faute":              "fautes",
    "Défenseur battu":    "def_battus",
    "Duel aérien":        "duels_aeriens",
    "Franchissement":     "franchissements",
    "Interception":       "interceptions",
    "Essai":              "essais",
    "Passe contact":      "passes_contact",
    "Grattage/Arrachage": "grattages",
    "Contest":            "contests",
    "Contre-Ruck":        "contre_rucks",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def normalize_match_name(name: str) -> str:
    """
    Unifie les noms de matchs entre fichiers.
    Convertit d'abord le format 'EQ1 vs EQ2' → 'EQ1-EQ2',
    puis applique les aliases pour corriger l'ordre des équipes.
    """
    if pd.isna(name):
        return None
    name = str(name).strip()
    # Conversion format "vs" → tiret
    if " vs " in name:
        name = name.replace(" vs ", "-")
    return MATCH_ALIASES.get(name, name)


def normalize_date(value) -> str | None:
    """Normalise une date au format ISO YYYY-MM-DD quelle que soit la source."""
    if not value or pd.isna(value):
        return None
    s = str(value).strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s


def snake(col: str) -> str:
    """Convertit un nom de colonne en snake_case."""
    col = str(col).strip()
    col = re.sub(r"[^\w\s]", "", col)
    col = re.sub(r"\s+", "_", col)
    return col.lower()


def load_gps_csv(filepath: Path) -> pd.DataFrame:
    """Charge un fichier CSV GPS STATSports et ajoute le nom de fichier source."""
    df = pd.read_csv(filepath)
    df["_source_file"] = filepath.name
    return df


def load_melees_csv(filepath: Path) -> pd.DataFrame:
    """
    Charge un fichier CSV mêlées STATSports et reconstruit un format plat.
    Le CSV est hiérarchique : la première ligne de chaque mêlée contient les
    infos de session, les lignes suivantes (un joueur par ligne) ont ces champs vides.
    On forward-fill les colonnes de niveau mêlée, puis on numérote chaque mêlée.
    """
    df = pd.read_csv(filepath, dtype=str)
    df["_source_file"] = filepath.name

    # Colonnes de niveau mêlée (remplies uniquement sur la 1re ligne de chaque mêlée)
    scrum_cols = ["Session", "Date", "Drill", "Start Time", "End Time", "Duration",
                  "Average Total Impact", "Average Front Row Impact",
                  "Average Second Row Impact", "Average Back Row Impact", "Scrum Sync Time"]

    # Numéroter chaque mêlée : nouvelle mêlée quand Session est non vide
    df["scrum_num"] = df["Session"].notna() & (df["Session"].str.strip() != "")
    df["scrum_num"] = df["scrum_num"].cumsum()

    # Forward-fill les infos de niveau mêlée vers les lignes joueurs
    df[scrum_cols] = df[scrum_cols].replace("", pd.NA).ffill()

    # Supprimer les lignes sans joueur (cas improbable mais défensif)
    df = df[df["Player"].notna() & (df["Player"].str.strip() != "")].copy()

    return df


def zone_start_cat(zone) -> str | None:
    """
    Catégorise la zone de départ d'un jeu au pied.
    Retourne : 'propre_22m', 'mi_terrain', 'camp_adverse' ou None.
    """
    if pd.isna(zone):
        return None
    z = str(zone)
    if "_adv" in z:
        return "camp_adverse"
    if "22m_50m" in z or "50m_22m" in z:
        return "mi_terrain"
    if "22m_" in z:
        return "propre_22m"
    return None


def zone_end_cat(zone) -> str | None:
    """
    Catégorise la zone d'arrivée d'un jeu au pied.
    Retourne : 'en_touche', 'camp_adverse', 'propre_ou_mi' ou None.
    """
    if pd.isna(zone):
        return None
    z = str(zone)
    if z.startswith("touche_"):
        return "en_touche"
    if "_adv" in z:
        return "camp_adverse"
    return "propre_ou_mi"


def parse_teams(title: str):
    """Extrait les deux équipes depuis un titre de type 'EQUIPE1-EQUIPE2'."""
    parts = title.split("-")
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return None, None


def normalize_actions_match(match_str: str):
    """
    Parse une chaîne actions de type 'DD-MM-YYYY: Nom Complet Eq1 - Nom Complet Eq2'.
    Retourne (date_iso, session_title_norm) ex: ('2026-03-07', 'REC-OMR').
    Les noms d'équipes inconnus sont conservés tels quels en guise d'abréviation.
    """
    if pd.isna(match_str):
        return None, None
    match_str = str(match_str).strip()
    # Séparer la date du reste : "DD-MM-YYYY: ..."
    if ": " not in match_str:
        return None, None
    date_raw, teams_str = match_str.split(": ", 1)
    # Séparer les deux équipes sur le premier " - "
    if " - " not in teams_str:
        return None, None
    team1_full, team2_full = teams_str.split(" - ", 1)
    team1 = TEAM_FULL_NAMES.get(team1_full.strip(), team1_full.strip())
    team2 = TEAM_FULL_NAMES.get(team2_full.strip(), team2_full.strip())
    try:
        date_iso = pd.to_datetime(date_raw, format="%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        date_iso = None
    session_title = normalize_match_name(f"{team1}-{team2}")
    return date_iso, session_title


# ── Chargement GPS matchs ──────────────────────────────────────────────────────
gps_match_files = sorted(GPS_MATCHS_DIR.glob("*.csv"))
dfs_match = []
for f in gps_match_files:
    df = load_gps_csv(f)
    print(f"📁 {f.name} → {len(df)} ligne(s) match")
    dfs_match.append(df)
df_match_raw = pd.concat(dfs_match, ignore_index=True) if dfs_match else pd.DataFrame()

# ── Chargement GPS entraînements ───────────────────────────────────────────────
gps_entr_files = sorted(GPS_ENTR_DIR.glob("*.csv"))
dfs_entr = []
for f in gps_entr_files:
    df = load_gps_csv(f)
    print(f"📁 {f.name} → {len(df)} ligne(s) entraînement")
    dfs_entr.append(df)
df_entr_raw = pd.concat(dfs_entr, ignore_index=True) if dfs_entr else pd.DataFrame()

# ── Chargement GPS mêlées ──────────────────────────────────────────────────────
gps_melees_files = sorted(GPS_MELEES_DIR.glob("*.csv"))
dfs_melees = []
for f in gps_melees_files:
    df = load_melees_csv(f)
    print(f"📁 {f.name} → {len(df)} ligne(s) mêlée")
    dfs_melees.append(df)
df_melees_raw = pd.concat(dfs_melees, ignore_index=True) if dfs_melees else pd.DataFrame()


# ── Connexion DB ──────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

print("✅ Connexion SQLite :", DB_PATH)

# Nettoyage des tables obsolètes (anciennes versions du pipeline)
for table_obsolete in ("actions_raw", "stats_match", "ratios", "perf_actions"):
    cur.execute(f"DROP TABLE IF EXISTS {table_obsolete}")
for view_obsolete in ():
    cur.execute(f"DROP VIEW IF EXISTS {view_obsolete}")
conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : JOUEURS
# Union de tous les joueurs présents dans les données match ET entraînement
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Construction JOUEURS…")

# Joueurs depuis GPS match + entraînement (ont le poste)
joueurs_gps = pd.concat([
    df_match_raw[["Player Last Name", "Player Primary Position"]],
    df_entr_raw[["Player Last Name", "Player Primary Position"]],
], ignore_index=True).dropna(subset=["Player Last Name"])

# Joueurs depuis mêlées (noms en majuscules, pas de poste)
if not df_melees_raw.empty:
    joueurs_mel = pd.DataFrame({
        "Player Last Name": df_melees_raw["Player"].str.strip().str.upper().dropna().unique(),
        "Player Primary Position": pd.NA,
    })
else:
    joueurs_mel = pd.DataFrame(columns=["Player Last Name", "Player Primary Position"])

# Joueurs depuis collisions (lecture anticipée de la colonne Player uniquement)
col_players = []
for f in sorted(GPS_COLLISIONS_DIR.glob("*.csv")):
    try:
        col_players.extend(pd.read_csv(f, usecols=["Player"])["Player"].dropna().tolist())
    except Exception:
        pass
if col_players:
    joueurs_col = pd.DataFrame({
        "Player Last Name": pd.Series(col_players).str.strip().str.upper().unique(),
        "Player Primary Position": pd.NA,
    })
else:
    joueurs_col = pd.DataFrame(columns=["Player Last Name", "Player Primary Position"])

joueurs = (
    pd.concat([joueurs_gps, joueurs_mel, joueurs_col], ignore_index=True)
    .dropna(subset=["Player Last Name"])
    .drop_duplicates(subset=["Player Last Name"])
    .rename(columns={
        "Player Last Name":        "nom",
        "Player Primary Position": "poste_principal",
    })
    .reset_index(drop=True)
)
joueurs.index += 1
joueurs.index.name = "joueur_id"
joueurs = joueurs.reset_index()

joueurs.to_sql("joueur", conn, if_exists="replace", index=False)
print(f"  → {len(joueurs)} joueurs insérés")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : MATCHS
# Pour chaque fichier CSV contenant des lignes Session Type == Match,
# on vérifie si le Session Title existe déjà dans la table matchs.
# Si non, on l'ajoute.
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Construction MATCHS…")

# Initialisation de la table match vide
cur.execute("DROP TABLE IF EXISTS match")
cur.execute("""
    CREATE TABLE match (
        match_id               INTEGER PRIMARY KEY,
        session_title TEXT,
        date                   TEXT,
        equipe_dom             TEXT,
        equipe_ext             TEXT,
        adversaire             TEXT,
        competition            TEXT
    )
""")
conn.commit()

match_id_counter = 1

# Titres depuis GPS matchs (avec date extraite de drill_date)
for f in gps_match_files:
    df = load_gps_csv(f)
    for title_raw in df["Session Title"].dropna().unique():
        title_norm = normalize_match_name(title_raw)
        exists = cur.execute(
            "SELECT 1 FROM match WHERE session_title = ?", (title_norm,)
        ).fetchone()
        date_series = df.loc[df["Session Title"] == title_raw, "Drill Date"].dropna()
        date_val = normalize_date(date_series.iloc[0] if not date_series.empty else None)
        if not exists:
            eq_dom, eq_ext = parse_teams(title_norm)
            adversaire = eq_dom if eq_ext == "REC" else eq_ext
            cur.execute(
                "INSERT INTO match VALUES (?, ?, ?, ?, ?, ?, ?)",
                (match_id_counter, title_norm, date_val, eq_dom, eq_ext, adversaire, "Nationale 1")
            )
            print(f"  + match ajouté (GPS) : {title_norm} (id={match_id_counter})")
            match_id_counter += 1
        elif date_val:
            cur.execute(
                "UPDATE match SET date = ? WHERE session_title = ? AND (date IS NULL OR date = '')",
                (date_val, title_norm)
            )

# Titres depuis GPS mêlées (matchs absents des fichiers GPS)
if not df_melees_raw.empty:
    for session_raw in df_melees_raw["Session"].dropna().unique():
        session_norm = normalize_match_name(session_raw)
        date_series_mel = df_melees_raw.loc[df_melees_raw["Session"] == session_raw, "Date"].dropna()
        date_val = normalize_date(date_series_mel.iloc[0] if not date_series_mel.empty else None)
        exists = cur.execute(
            "SELECT 1 FROM match WHERE session_title = ?", (session_norm,)
        ).fetchone()
        if not exists:
            eq_dom, eq_ext = parse_teams(session_norm)
            adversaire = eq_dom if eq_ext == "REC" else eq_ext
            cur.execute(
                "INSERT INTO match VALUES (?, ?, ?, ?, ?, ?, ?)",
                (match_id_counter, session_norm, date_val, eq_dom, eq_ext, adversaire, "Nationale 1")
            )
            print(f"  + match ajouté (mêlées) : {session_norm} — {date_val} (id={match_id_counter})")
            match_id_counter += 1
        elif date_val:
            cur.execute(
                "UPDATE match SET date = ? WHERE session_title = ? AND (date IS NULL OR date = '')",
                (date_val, session_norm)
            )

# Titres depuis fichiers de codage actions (matchs absents des sources précédentes)
for f in sorted(ACTIONS_DIR.glob("*.csv")):
    try:
        df_tmp = pd.read_csv(f, usecols=["match"])
    except Exception as e:
        print(f"  ⚠️  {f.name} ignoré : {e}")
        continue
    for match_raw in df_tmp["match"].dropna().unique():
        date_val, session_norm = normalize_actions_match(match_raw)
        if not session_norm:
            continue
        exists = cur.execute(
            "SELECT 1 FROM match WHERE session_title = ?", (session_norm,)
        ).fetchone()
        if not exists:
            eq_dom, eq_ext = parse_teams(session_norm)
            adversaire = eq_dom if eq_ext == "REC" else eq_ext
            cur.execute(
                "INSERT INTO match VALUES (?, ?, ?, ?, ?, ?, ?)",
                (match_id_counter, session_norm, date_val, eq_dom, eq_ext, adversaire, "Nationale 1")
            )
            print(f"  + match ajouté (actions) : {session_norm} — {date_val} (id={match_id_counter})")
            match_id_counter += 1
        elif date_val:
            cur.execute(
                "UPDATE match SET date = ? WHERE session_title = ? AND (date IS NULL OR date = '')",
                (date_val, session_norm)
            )

# Titres depuis GPS collisions (matchs absents des sources précédentes)
for f in sorted(GPS_COLLISIONS_DIR.glob("*.csv")):
    try:
        df_tmp = pd.read_csv(f, usecols=["Session", "Date"])
    except Exception as e:
        print(f"  ⚠️  {f.name} ignoré : {e}")
        continue
    for session_raw in df_tmp["Session"].dropna().unique():
        session_norm = normalize_match_name(session_raw)
        date_series = df_tmp.loc[df_tmp["Session"] == session_raw, "Date"].dropna()
        date_val = normalize_date(date_series.iloc[0] if not date_series.empty else None)
        exists = cur.execute(
            "SELECT 1 FROM match WHERE session_title = ?", (session_norm,)
        ).fetchone()
        if not exists:
            eq_dom, eq_ext = parse_teams(session_norm)
            adversaire = eq_dom if eq_ext == "REC" else eq_ext
            cur.execute(
                "INSERT INTO match VALUES (?, ?, ?, ?, ?, ?, ?)",
                (match_id_counter, session_norm, date_val, eq_dom, eq_ext, adversaire, "Nationale 1")
            )
            print(f"  + match ajouté (collisions) : {session_norm} — {date_val} (id={match_id_counter})")
            match_id_counter += 1
        elif date_val:
            cur.execute(
                "UPDATE match SET date = ? WHERE session_title = ? AND (date IS NULL OR date = '')",
                (date_val, session_norm)
            )

conn.commit()

matchs = pd.read_sql("SELECT * FROM match", conn)
print(f"  → {len(matchs)} match(s) en base")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : PERF_MATCH
# 1 ligne = 1 joueur × 1 match (GPS STATSports + actions codage vidéo)
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement PERF_MATCH…")

dfs_pm = []
for f in gps_match_files:
    df_f = load_gps_csv(f)
    df_parsed = p_gps_matchs.parse(df_f, joueurs, matchs, filename=f.name)
    dfs_pm.append(df_parsed)
df_pm = pd.concat(dfs_pm, ignore_index=True) if dfs_pm else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : PERF_ENTRAINEMENT
# 1 ligne = 1 joueur × 1 drill d'entraînement
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement PERF_ENTRAINEMENT…")
print(f"  → {len(df_entr_raw)} lignes brutes")

dfs_entr_parsed = []
for f in gps_entr_files:
    df_f = load_gps_csv(f)
    df_parsed = p_gps_entr.parse(df_f, joueurs, matchs, filename=f.name)
    dfs_entr_parsed.append(df_parsed)
df_entr = pd.concat(dfs_entr_parsed, ignore_index=True) if dfs_entr_parsed else pd.DataFrame()
df_entr.insert(0, "perf_entr_id", range(1, len(df_entr) + 1))

df_entr.to_sql("perf_entrainement", conn, if_exists="replace", index=False)
print(f"  → {len(df_entr)} lignes perf_entrainement insérées")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : MELEES
# 1 ligne = 1 joueur × 1 mêlée
# Source : data/raw/gps_melee/*.csv
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement MELEES…")

if not df_melees_raw.empty:
    dfs_mel_parsed = []
    for f in gps_melees_files:
        df_f = load_melees_csv(f)
        df_parsed = p_gps_mel.parse(df_f, joueurs, matchs, filename=f.name)
        dfs_mel_parsed.append(df_parsed)
    df_mel = pd.concat(dfs_mel_parsed, ignore_index=True) if dfs_mel_parsed else pd.DataFrame()
    df_mel.insert(0, "melee_id", range(1, len(df_mel) + 1))
    df_mel.to_sql("melee", conn, if_exists="replace", index=False)
    print(f"  → {len(df_mel)} lignes melee insérées")
else:
    print("  ⚠️  Aucun fichier mêlées trouvé")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : COLLISIONS
# 1 ligne = 1 collision d'un joueur lors d'un match
# Source : data/raw/gps_collision/*.csv
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement COLLISIONS…")

collision_files = sorted(GPS_COLLISIONS_DIR.glob("*.csv"))
dfs_col = []
for f in collision_files:
    try:
        df_c = pd.read_csv(f)
        df_c["_source_file"] = f.name
        dfs_col.append(df_c)
    except Exception as e:
        print(f"  ⚠️  {f.name} ignoré : {e}")

if dfs_col:
    dfs_col_parsed = []
    for f in collision_files:
        df_f = pd.read_csv(f)
        df_f["_source_file"] = f.name
        df_parsed = p_gps_col.parse(df_f, joueurs, matchs, filename=f.name)
        dfs_col_parsed.append(df_parsed)
    df_col = pd.concat(dfs_col_parsed, ignore_index=True) if dfs_col_parsed else pd.DataFrame()
    df_col.insert(0, "collision_id", range(1, len(df_col) + 1))
    df_col.to_sql("collision", conn, if_exists="replace", index=False)
    print(f"  → {len(df_col)} lignes collision insérées")
else:
    print("  ⚠️  Aucun fichier collisions trouvé")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : PERF_ACTIONS + TOUCHES + ENRICHISSEMENT MATCHS
# Source : data/raw/actions_match/*.csv
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement PERF_ACTIONS…")

actions_files = sorted(ACTIONS_DIR.glob("*.csv"))
dfs_actions = []
for f in actions_files:
    try:
        df_a = pd.read_csv(f)
        df_a["_source_file"] = f.name
        dfs_actions.append(df_a)
    except Exception as e:
        print(f"  ⚠️  {f.name} ignoré : {e}")

if dfs_actions:
    df_raw_act = pd.concat(dfs_actions, ignore_index=True)
    parsed_result = p_actions.parse(df_raw_act, joueurs, matchs,
                                    filename=", ".join(f.name for f in actions_files))
    df_perf_actions = parsed_result["perf_match_actions"]
    df_touches_act  = parsed_result["touche"]
    df_matchs_stats = parsed_result["matchs_stats"]
else:
    df_perf_actions = pd.DataFrame(columns=["match_id", "joueur_id"])
    df_touches_act  = pd.DataFrame()
    df_matchs_stats = pd.DataFrame()

# ── Fusion GPS + actions → perf_match ─────────────────────────────────────────
print("\n📥 Fusion PERF_MATCH (GPS + actions)…")

df_perf_match = df_pm.merge(
    df_perf_actions, on=["joueur_id", "match_id"], how="outer"
)
df_perf_match.insert(0, "perf_match_id", range(1, len(df_perf_match) + 1))

df_perf_match.to_sql("perf_match", conn, if_exists="replace", index=False)
print(f"  → {len(df_perf_match)} lignes perf_match insérées")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE : TOUCHES
# 1 ligne = 1 touche (action "Touche" du codage vidéo)
# Inclut REC et équipes adverses — permet la comparaison par match
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Chargement TOUCHES…")

if not df_touches_act.empty:
    df_t = df_touches_act.copy()
    df_t.insert(0, "touche_id", range(1, len(df_t) + 1))
    df_t.to_sql("touche", conn, if_exists="replace", index=False)
    print(f"  → {len(df_t)} lignes touche insérées")
    rec_t  = df_t["est_rec"].sum()
    adv_t  = (df_t["est_rec"] == 0).sum()
    sans_m = df_t["match_id"].isna().sum()
    print(f"     REC: {rec_t} | Adversaires: {adv_t}")
    if sans_m:
        print(f"  ⚠️  {sans_m} ligne(s) sans match_id")
else:
    print("  ⚠️  Aucun fichier actions trouvé")


# ══════════════════════════════════════════════════════════════════════════════
# ENRICHISSEMENT TABLE MATCHS — métriques collectives REC et adversaire
# ══════════════════════════════════════════════════════════════════════════════
print("\n📥 Enrichissement MATCHS avec métriques collectives…")

if not df_matchs_stats.empty:
    df_matchs_enrichi = pd.read_sql("SELECT * FROM match", conn)
    df_matchs_enrichi = df_matchs_enrichi.merge(df_matchs_stats, on="match_id", how="left")
    cols_stats = [c for c in df_matchs_enrichi.columns
                  if c not in ("match_id", "session_title", "date",
                               "equipe_dom", "equipe_ext", "adversaire", "competition")]
    df_matchs_enrichi[cols_stats] = df_matchs_enrichi[cols_stats].fillna(0).astype(int)
    df_matchs_enrichi.to_sql("match", conn, if_exists="replace", index=False)
    print(f"  → {len(cols_stats)} colonnes de métriques ajoutées à match")
else:
    print("  ⚠️  Aucun fichier actions trouvé — match non enrichie")


# ══════════════════════════════════════════════════════════════════════════════
# VUES SQL utiles
# ══════════════════════════════════════════════════════════════════════════════
print("\n🔧 Création des vues SQL…")

cur.executescript("""
-- Vue : collision agrégées par joueur × match
DROP VIEW IF EXISTS view_collisions_par_match;
CREATE VIEW view_collisions_par_match AS
SELECT
    c.joueur_id,
    j.nom AS joueur,
    c.match_id,
    m.session_title AS match_titre,
    COUNT(*) AS nb_collisions,
    AVG(CAST(c.collision_load AS REAL)) AS moy_collision_load,
    MAX(CAST(c.collision_load AS REAL)) AS max_collision_load,
    AVG(CAST(c.time_to_feet AS REAL)) AS moy_time_to_feet,
    SUM(CAST(c.post_collision_accel AS REAL)) AS total_post_accels
FROM collision c
LEFT JOIN joueur j ON c.joueur_id = j.joueur_id
LEFT JOIN match m ON c.match_id = m.match_id
GROUP BY c.joueur_id, c.match_id;

-- Vue : total par joueur × match (agrégation des 2 mi-temps)
DROP VIEW IF EXISTS view_match_total;
CREATE VIEW view_match_total AS
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

-- Vue : charge hebdomadaire par joueur (entraînement)
DROP VIEW IF EXISTS view_charge_hebdo;
CREATE VIEW view_charge_hebdo AS
SELECT
    joueur_id,
    strftime('%Y-%W', date) AS semaine,
    session_type,
    COUNT(*) AS nb_sessions,
    SUM(CAST(total_distance AS REAL)) AS total_distance,
    SUM(CAST(accelerations_absolute AS INTEGER)) AS total_accels,
    SUM(CAST(dynamic_stress_load AS REAL)) AS total_dsl
FROM perf_entrainement
WHERE date IS NOT NULL
GROUP BY joueur_id, semaine, session_type;
""")

conn.commit()
print("  → 3 vues créées : view_match_total, view_collisions_par_match, view_charge_hebdo")


# ══════════════════════════════════════════════════════════════════════════════
# RAPPORT DE QUALITÉ
# ══════════════════════════════════════════════════════════════════════════════
print("\n📊 Rapport de qualité des jointures :\n")

tables = {
    "joueur":                        "SELECT COUNT(*) FROM joueur",
    "match":                         "SELECT COUNT(*) FROM match",
    "perf_match":                    "SELECT COUNT(*) FROM perf_match",
    "perf_match (sans joueur_id)":   "SELECT COUNT(*) FROM perf_match WHERE joueur_id IS NULL",
    "perf_match (sans match_id)":    "SELECT COUNT(*) FROM perf_match WHERE match_id IS NULL",
    "perf_entrainement":             "SELECT COUNT(*) FROM perf_entrainement",
    "perf_entr (sans joueur_id)":    "SELECT COUNT(*) FROM perf_entrainement WHERE joueur_id IS NULL",
    "melee":                           "SELECT COUNT(*) FROM melee",
    "melee (sans joueur_id)":          "SELECT COUNT(*) FROM melee WHERE joueur_id IS NULL",
    "melee (sans match_id)":           "SELECT COUNT(*) FROM melee WHERE match_id IS NULL",
    "collision":                       "SELECT COUNT(*) FROM collision",
    "collision (sans joueur_id)":      "SELECT COUNT(*) FROM collision WHERE joueur_id IS NULL",
    "collision (sans match_id)":       "SELECT COUNT(*) FROM collision WHERE match_id IS NULL",
    "collision (mi_temps inconnu)":    "SELECT COUNT(*) FROM collision WHERE mi_temps IS NULL",
    "perf_match (sans GPS)":           "SELECT COUNT(*) FROM perf_match WHERE _source_file_gps IS NULL",
    "perf_match (sans actions)":       "SELECT COUNT(*) FROM perf_match WHERE _source_file_actions IS NULL",
    "touche":                          "SELECT COUNT(*) FROM touche",
    "touche REC":                      "SELECT COUNT(*) FROM touche WHERE est_rec = 1",
    "touche adversaires":              "SELECT COUNT(*) FROM touche WHERE est_rec = 0",
    "touche (sans match_id)":          "SELECT COUNT(*) FROM touche WHERE match_id IS NULL",
    "match (colonnes métriques)":      "SELECT COUNT(*) FROM pragma_table_info('match') WHERE name LIKE '%_rec' OR name LIKE '%_adv'",
}

for label, query in tables.items():
    try:
        count = cur.execute(query).fetchone()[0]
        flag = " ⚠️" if "sans" in label and count > 0 else ""
        print(f"  {label:<42} : {count:>6}{flag}")
    except Exception as e:
        print(f"  {label:<42} : erreur ({e})")

conn.close()
print(f"\n✅ Pipeline terminé — base : {DB_PATH}")
