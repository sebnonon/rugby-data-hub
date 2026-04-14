"""
Parser STATSports — GPS Entraînement
Nomenclature attendue : gps_entrainement_YYYY-MM-DD_(Reprise|J-2|J-1|J+2).csv
Table cible           : perf_entrainement
"""

import re
import pandas as pd
from .utils import normalize_date, snake, normalize_session_type, normalize_seance_type, assign_fk, SESSION_TYPES_VALIDES

FILE_TYPE = "gps_entrainement"

_TYPES = "|".join(re.escape(t) for t in SESSION_TYPES_VALIDES)
NOMENCLATURE_PATTERN = re.compile(
    rf"^gps_entrainement_(\d{{4}}-\d{{2}}-\d{{2}})_({_TYPES})\.csv$"
)

REQUIRED_COLUMNS = []

# Colonnes alternatives selon la version STATSports
PLAYER_NAME_COLS = ["Player Last Name", "Player Display Name"]
DISTANCE_COLS    = ["Total Distance", "Total Distance (m)"]

ENTR_KEEP_COLS = [
    "joueur_id", "player_name",
    "seance_type", "session_type", "titre", "date",
    "total_time", "total_distance", "distance_per_min", "max_speed",
    "sprints", "sprint_distance",
    "high_speed_running_absolute", "hsr_per_minute_absolute",
    "hml_distance", "hmld_per_minute",
    "accelerations_absolute", "decelerations_absolute",
    "dynamic_stress_load", "metabolic_distance_absolute",
    "max_acceleration",
    "acute", "chronic", "acute_chronic_ratio",
    "_source_file",
]


def validate_filename(filename: str) -> tuple[bool, str, dict]:
    """
    Valide le nom de fichier selon la nomenclature.
    Retourne (valide, message_erreur, métadonnées).
    """
    m = NOMENCLATURE_PATTERN.match(filename)
    if not m:
        types_str = ", ".join(sorted(SESSION_TYPES_VALIDES))
        return (
            False,
            f"Nom invalide. Format attendu : gps_entrainement_YYYY-MM-DD_TYPE.csv\n"
            f"Types valides : {types_str}\n"
            f"Exemple : gps_entrainement_2025-11-14_J-2.csv",
            {},
        )
    return True, "", {"date": m.group(1), "session_type": m.group(2)}


def parse(df_raw: pd.DataFrame,
          joueurs_df: pd.DataFrame,
          matchs_df: pd.DataFrame,
          filename: str = "") -> pd.DataFrame:
    """
    Parse un DataFrame brut STATSports GPS entraînements.
    Retourne un DataFrame prêt pour l'insertion dans perf_entrainement.
    """
    df = df_raw.copy()
    df["_source_file"] = filename

    # Accepte "Player Last Name" ou "Player Display Name" selon la version STATSports
    player_col = next((c for c in PLAYER_NAME_COLS if c in df.columns), None)
    if player_col and player_col != "Player Last Name":
        df = df.rename(columns={player_col: "Player Last Name"})

    # Accepte "Total Distance (m)" ou "Total Distance"
    dist_col = next((c for c in DISTANCE_COLS if c in df.columns), None)
    if dist_col and dist_col != "Total Distance":
        df = df.rename(columns={dist_col: "Total Distance"})

    df["player_name"] = df["Player Last Name"].str.strip().str.upper()
    df = assign_fk(df, joueurs_df, matchs_df, session_col="__no_match__")

    # Renommage snake_case
    df = df.rename(columns={c: snake(c) for c in df.columns})

    # Normalisation des types de séance
    if "session_type" in df.columns:
        df["session_type"] = df["session_type"].apply(normalize_session_type)
    if "session_title" in df.columns:
        df["seance_type"] = df["session_title"].apply(normalize_seance_type)

    # Renommage colonnes finales
    rename_map = {}
    if "drill_date" in df.columns:
        rename_map["drill_date"] = "date"
    if "drill_title" in df.columns:
        rename_map["drill_title"] = "titre"
    df = df.rename(columns=rename_map)

    if "date" in df.columns:
        df["date"] = df["date"].apply(normalize_date)

    available = [c for c in ENTR_KEEP_COLS if c in df.columns]
    return df[available].copy()
