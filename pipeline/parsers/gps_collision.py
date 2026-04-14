"""
Parser STATSports — GPS Collision
Nomenclature attendue : gps_collision_EQ1-EQ2_YYYY-MM-DD.csv
Table cible           : collision
"""

import re
import pandas as pd
from .utils import normalize_match_name, assign_fk

FILE_TYPE = "gps_collision"

NOMENCLATURE_PATTERN = re.compile(
    r"^gps_collision_([A-Z0-9]+-[A-Z0-9]+)_(\d{4}-\d{2}-\d{2})\.csv$"
)

REQUIRED_COLUMNS = [
    "Session",
    "Player",
    "Collision Load",
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
            f"Nom invalide. Format attendu : gps_collision_EQ1-EQ2_YYYY-MM-DD.csv\n"
            f"Exemple : gps_collision_REC-NRC_2025-09-13.csv",
            {},
        )
    return True, "", {"session_title": m.group(1), "date": m.group(2)}


def parse(df_raw: pd.DataFrame,
          joueurs_df: pd.DataFrame,
          matchs_df: pd.DataFrame,
          filename: str = "") -> pd.DataFrame:
    """
    Parse un DataFrame brut STATSports collisions.
    Retourne un DataFrame prêt pour l'insertion dans collisions.
    """
    df = df_raw.copy()
    df["_source_file"] = filename

    # Supprimer les lignes sans session ni joueur (agrégats en fin de fichier)
    df = df.dropna(subset=["Session", "Player"]).copy()

    df["session_title_norm"] = df["Session"].apply(normalize_match_name)
    df["player_name"]        = df["Player"].str.strip().str.upper()

    df = assign_fk(df, joueurs_df, matchs_df)

    # Extraction mi-temps depuis la colonne Drill
    if "Drill" in df.columns:
        df["mi_temps"] = df["Drill"].str.extract(r"(\d)")[0].astype("Int64")

    df = df.rename(columns={
        "Start Time":                   "start_time",
        "End Time":                     "end_time",
        "Duration":                     "duration",
        "Collision Load":               "collision_load",
        "Time To Feet":                 "time_to_feet",
        "Post Collision Accelerations": "post_collision_accel",
    })

    keep_cols = [
        "joueur_id", "match_id", "mi_temps",
        "start_time", "end_time", "duration",
        "collision_load", "time_to_feet", "post_collision_accel",
        "_source_file",
    ]
    available = [c for c in keep_cols if c in df.columns]
    return df[available].copy()
