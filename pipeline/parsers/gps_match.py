"""
Parser STATSports — GPS Match
Nomenclature attendue : gps_match_EQ1-EQ2_YYYY-MM-DD.csv
Table cible           : perf_match (colonnes GPS uniquement)
"""

import re
import pandas as pd
from .utils import normalize_match_name, snake, assign_fk

FILE_TYPE = "gps_match"

NOMENCLATURE_PATTERN = re.compile(
    r"^gps_match_([A-Z0-9]+-[A-Z0-9]+)_(\d{4}-\d{2}-\d{2})\.csv$"
)

REQUIRED_COLUMNS = [
    "Session Title",
]

# Colonnes alternatives selon la version STATSports
PLAYER_NAME_COLS = ["Player Last Name", "Player Display Name"]
DISTANCE_COLS    = ["Total Distance", "Total Distance (m)"]

GPS_KEEP_COLS = [
    "joueur_id", "match_id",
    "total_time", "average_speed", "total_distance", "distance_per_min",
    "sprints", "sprint_distance", "max_speed",
    "high_speed_running_absolute", "hsr_per_minute_absolute",
    "hml_distance", "hmld_per_minute",
    "accelerations_absolute", "decelerations_absolute",
    "accelerations_per_min_absolute", "decels_per_min_absolute",
    "step_balance", "collision_load", "collisions",
    "dynamic_stress_load", "metabolic_distance_absolute",
    "average_metabolic_power", "max_acceleration", "max_deceleration",
    "acute", "chronic", "acute_chronic_ratio",
    "_source_file_gps",
]


def validate_filename(filename: str) -> tuple[bool, str, dict]:
    """
    Valide le nom de fichier selon la nomenclature.
    Retourne (valide, message_erreur, métadonnées).
    """
    m = NOMENCLATURE_PATTERN.match(filename)
    if not m:
        return (
            False,
            f"Nom invalide. Format attendu : gps_match_EQ1-EQ2_YYYY-MM-DD.csv\n"
            f"Exemple : gps_match_REC-OMR_2025-11-15.csv",
            {},
        )
    return True, "", {"session_title": m.group(1), "date": m.group(2)}


def parse(df_raw: pd.DataFrame,
          joueurs_df: pd.DataFrame,
          matchs_df: pd.DataFrame,
          filename: str = "") -> pd.DataFrame:
    """
    Parse un DataFrame brut STATSports GPS matchs.
    Retourne un DataFrame prêt pour l'insertion dans perf_match.
    """
    df = df_raw.copy()
    df["_source_file_gps"] = filename

    # Accepte "Player Last Name" ou "Player Display Name" selon la version STATSports
    player_col = next((c for c in PLAYER_NAME_COLS if c in df.columns), None)
    if player_col and player_col != "Player Last Name":
        df = df.rename(columns={player_col: "Player Last Name"})

    # Accepte "Total Distance (m)" ou "Total Distance"
    dist_col = next((c for c in DISTANCE_COLS if c in df.columns), None)
    if dist_col and dist_col != "Total Distance":
        df = df.rename(columns={dist_col: "Total Distance"})

    df["player_name"]        = df["Player Last Name"].str.strip().str.upper()
    df["session_title_norm"] = df["Session Title"].apply(normalize_match_name)

    df = assign_fk(df, joueurs_df, matchs_df)

    # Renommage snake_case
    df = df.rename(columns={c: snake(c) for c in df.columns})
    # _source_file_gps a pu être renommé par snake()
    if "_source_file_gps" not in df.columns and "source_file_gps" in df.columns:
        df = df.rename(columns={"source_file_gps": "_source_file_gps"})

    available = [c for c in GPS_KEEP_COLS if c in df.columns]
    return df[available].copy()
