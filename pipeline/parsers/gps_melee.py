"""
Parser STATSports — GPS Mêlée
Nomenclature attendue : gps_melee_EQ1-EQ2_YYYY-MM-DD.csv
Table cible           : melee

Format source particulier : CSV hiérarchique — la première ligne de chaque mêlée
contient les infos de session, les lignes joueurs suivantes ont ces champs vides.
On forward-fill les colonnes de niveau mêlée.
"""

import re
import pandas as pd
from .utils import normalize_match_name, assign_fk

FILE_TYPE = "gps_melee"

NOMENCLATURE_PATTERN = re.compile(
    r"^gps_melee_([A-Z0-9]+-[A-Z0-9]+)_(\d{4}-\d{2}-\d{2})\.csv$"
)

REQUIRED_COLUMNS = [
    "Session",
    "Player",
    "Impact",
]

# Colonnes de niveau mêlée (remplies uniquement sur la 1re ligne de chaque mêlée)
SCRUM_LEVEL_COLS = [
    "Session", "Date", "Drill", "Start Time", "End Time", "Duration",
    "Average Total Impact", "Average Front Row Impact",
    "Average Second Row Impact", "Average Back Row Impact", "Scrum Sync Time",
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
            f"Nom invalide. Format attendu : gps_melee_EQ1-EQ2_YYYY-MM-DD.csv\n"
            f"Exemple : gps_melee_REC-CSBJ_2025-11-08.csv",
            {},
        )
    return True, "", {"session_title": m.group(1), "date": m.group(2)}


def load_and_flatten(df_raw: pd.DataFrame, filename: str = "") -> pd.DataFrame:
    """
    Aplatit le format hiérarchique STATSports mêlées :
    forward-fill des infos de session + numérotation des mêlées.
    """
    df = df_raw.copy().astype(str).replace("nan", pd.NA)
    df["_source_file"] = filename

    # Numéroter chaque mêlée : nouvelle mêlée quand Session est non vide
    df["scrum_num"] = df["Session"].notna() & (df["Session"].str.strip() != "")
    df["scrum_num"] = df["scrum_num"].cumsum()

    # Forward-fill les infos de niveau mêlée
    scrum_cols_present = [c for c in SCRUM_LEVEL_COLS if c in df.columns]
    df[scrum_cols_present] = df[scrum_cols_present].replace("", pd.NA).ffill()

    # Supprimer les lignes sans joueur
    df = df[df["Player"].notna() & (df["Player"].str.strip() != "")].copy()

    return df


def parse(df_raw: pd.DataFrame,
          joueurs_df: pd.DataFrame,
          matchs_df: pd.DataFrame,
          filename: str = "") -> pd.DataFrame:
    """
    Parse un DataFrame brut STATSports mêlées.
    Retourne un DataFrame prêt pour l'insertion dans melee.
    """
    df = load_and_flatten(df_raw, filename)

    df["session_norm"] = df["Session"].apply(normalize_match_name)
    df["player_name"]  = df["Player"].str.strip().str.upper()

    df = assign_fk(df, joueurs_df, matchs_df,
                   player_col="player_name", session_col="session_norm")

    # Extraction mi-temps
    if "Drill" in df.columns:
        df["mi_temps"] = df["Drill"].str.extract(r"(\d)")[0].astype("Int64")

    df = df.rename(columns={
        "Start Time":                "start_time",
        "End Time":                  "end_time",
        "Duration":                  "duration",
        "Average Total Impact":      "avg_total_impact",
        "Average Front Row Impact":  "avg_front_row_impact",
        "Average Second Row Impact": "avg_second_row_impact",
        "Average Back Row Impact":   "avg_back_row_impact",
        "Scrum Sync Time":           "scrum_sync_time",
        "Impact":                    "impact",
        "Sync Time":                 "sync_time",
        "Scrum Load":                "scrum_load",
        "Time To Feet":              "time_to_feet",
        "Post Scrum Accel":          "post_scrum_accel",
    })

    keep_cols = [
        "joueur_id", "match_id", "scrum_num", "mi_temps",
        "start_time", "end_time", "duration",
        "avg_total_impact", "avg_front_row_impact",
        "avg_second_row_impact", "avg_back_row_impact",
        "scrum_sync_time", "impact", "sync_time", "scrum_load",
        "time_to_feet", "post_scrum_accel",
        "_source_file",
    ]
    available = [c for c in keep_cols if c in df.columns]
    df = df[available].copy()

    # scrum_id : identifiant unique par mêlée partagé entre tous les joueurs
    df["scrum_id"] = df.groupby(["match_id", "scrum_num"], sort=False).ngroup() + 1

    return df
