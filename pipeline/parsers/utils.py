"""
Utilitaires partagés entre les parsers STATSports.
"""

import re
import pandas as pd
from datetime import datetime

# ── Configuration club ────────────────────────────────────────────────────────
# Remplacer par l'abréviation de votre club (ex: "PAU", "AGEN", "BRIVE")
HOME_TEAM = "REC"

# ── Constantes de mapping ─────────────────────────────────────────────────────

MATCH_ALIASES = {
    f"RNR-{HOME_TEAM}": f"{HOME_TEAM}-RNR",
    f"OMR-{HOME_TEAM}": f"{HOME_TEAM}-OMR",
    f"NRC-{HOME_TEAM}": f"{HOME_TEAM}-NRC",
}

PLAYER_ALIASES = {
    "TUUGAHALA O": "TUUGAHALA",
}

TEAM_FULL_NAMES = {
    "Rennes Etudiants Club":          HOME_TEAM,
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

# Actions de codage vidéo → préfixes de colonnes perf_match
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

# Types de séance d'entraînement autorisés
SESSION_TYPES_VALIDES = {"Reprise", "J-2", "J-1", "J+2"}

# ── Fonctions helpers ─────────────────────────────────────────────────────────

def normalize_match_name(name: str) -> str | None:
    """Unifie les noms de matchs entre fichiers (format EQ1-EQ2)."""
    if pd.isna(name):
        return None
    name = str(name).strip()
    if " vs " in name:
        name = name.replace(" vs ", "-")
    return MATCH_ALIASES.get(name, name)


def normalize_date(value) -> str | None:
    """Normalise une date au format ISO YYYY-MM-DD."""
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


def normalize_session_type(val: str) -> str:
    """Normalise le session_type STATSports : 'Jeudi J -1' → 'J-1'."""
    if pd.isna(val):
        return val
    m = re.search(r"J\s*([+-])\s*(\d+)", str(val), re.IGNORECASE)
    if m:
        return f"J{m.group(1)}{m.group(2)}"
    return str(val).strip()


JOURS_SEMAINE = {"lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"}
SEANCE_ENCODAGE = {r"s.par.": "séparé", r"mix.": "mixé"}

def normalize_seance_type(val: str) -> str:
    """Extrait le type de séance depuis le session_title STATSports."""
    if pd.isna(val):
        return val
    tokens = str(val).strip().split()
    if tokens and tokens[0].lower() in JOURS_SEMAINE:
        tokens = tokens[1:]
    seance = tokens[0] if tokens else val
    for pattern, replacement in SEANCE_ENCODAGE.items():
        if re.fullmatch(pattern, seance, re.IGNORECASE):
            return replacement
    return seance.lower()


def normalize_actions_match(match_str: str) -> tuple[str | None, str | None]:
    """
    Parse une chaîne actions de type 'DD-MM-YYYY: Nom Complet Eq1 - Nom Complet Eq2'.
    Retourne (date_iso, session_title_norm).
    """
    if pd.isna(match_str):
        return None, None
    match_str = str(match_str).strip()
    if ": " not in match_str:
        return None, None
    date_raw, teams_str = match_str.split(": ", 1)
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


def zone_start_cat(zone) -> str | None:
    """Catégorise la zone de départ d'un jeu au pied."""
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
    """Catégorise la zone d'arrivée d'un jeu au pied."""
    if pd.isna(zone):
        return None
    z = str(zone)
    if z.startswith("touche_"):
        return "en_touche"
    if "_adv" in z:
        return "camp_adverse"
    return "propre_ou_mi"


def parse_teams(title: str) -> tuple[str | None, str | None]:
    """Extrait les deux équipes depuis un titre 'EQUIPE1-EQUIPE2'."""
    parts = title.split("-")
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return None, None


def assign_fk(df: pd.DataFrame, joueurs_df: pd.DataFrame, matchs_df: pd.DataFrame,
              player_col: str = "player_name",
              session_col: str = "session_title_norm") -> pd.DataFrame:
    """
    Assigne joueur_id et match_id par jointure sur les DataFrames de référence.
    Retourne le DataFrame enrichi.
    """
    if player_col in df.columns and not joueurs_df.empty:
        df[player_col] = df[player_col].replace(PLAYER_ALIASES)
        df = df.merge(joueurs_df[["joueur_id", "nom"]],
                      left_on=player_col, right_on="nom", how="left").drop(columns=["nom"], errors="ignore")
    if session_col in df.columns and not matchs_df.empty:
        df = df.merge(matchs_df[["match_id", "session_title"]],
                      left_on=session_col, right_on="session_title", how="left").drop(columns=["session_title"], errors="ignore")
    return df
