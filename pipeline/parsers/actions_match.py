"""
Parser STATSports — Actions match (codage vidéo)
Nomenclature attendue : actions_match_EQ1-EQ2_YYYY-MM-DD.csv
Tables cibles         : perf_match (colonnes actions), touche, match (métriques collectives)
"""

import re
import pandas as pd
from .utils import (
    normalize_actions_match, ACTIONS_STANDARD, TEAM_FULL_NAMES, ABBREV_TO_FULL_NAME,
    zone_start_cat, zone_end_cat, PLAYER_ALIASES, assign_fk, HOME_TEAM,
)

FILE_TYPE = "actions_match"

NOMENCLATURE_PATTERN = re.compile(
    r"^actions_match_([A-Z0-9]+-[A-Z0-9]+)_(\d{4}-\d{2}-\d{2})\.csv$"
)

REQUIRED_COLUMNS = [
    "match",
    "team",
    "action",
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
            f"Nom invalide. Format attendu : actions_match_EQ1-EQ2_YYYY-MM-DD.csv\n"
            f"Exemple : actions_match_REC-OMR_2025-11-15.csv",
            {},
        )
    return True, "", {"session_title": m.group(1), "date": m.group(2)}


def parse(df_raw: pd.DataFrame,
          joueurs_df: pd.DataFrame,
          matchs_df: pd.DataFrame,
          filename: str = "") -> dict[str, pd.DataFrame]:
    """
    Parse un DataFrame brut de codage vidéo actions matchs.
    Retourne un dict avec les DataFrames pour chaque table cible :
      - "perf_match_actions" : métriques actions agrégées par joueur × match
      - "touches"            : données de touche (REC + adversaire)
      - "matchs_stats"       : métriques collectives par match (mêlées, rucks, etc.)
    """
    df_raw = df_raw.copy()
    df_raw["_source_file"] = filename

    # ── Résolution match_id ────────────────────────────────────────────────────
    parsed = df_raw["match"].apply(normalize_actions_match)
    df_raw["_session_norm"] = [p[1] for p in parsed]
    df_raw = df_raw.merge(
        matchs_df[["match_id", "session_title"]],
        left_on="_session_norm", right_on="session_title", how="left"
    ).drop(columns=["_session_norm", "session_title"], errors="ignore")

    # ── Séparation actions avec joueurs / sans joueurs ────────────────────────
    player_cols = [f"player_{i}" for i in range(1, 24)]
    has_player  = df_raw[player_cols].notna().any(axis=1)
    df_avec_joueur = df_raw[has_player].copy()
    df_sans_joueur = df_raw[~has_player].copy()

    # ── Perf actions (actions individuelles) ──────────────────────────────────
    df_perf_actions = _parse_perf_actions(df_avec_joueur, joueurs_df, filename)

    # ── Touches ───────────────────────────────────────────────────────────────
    df_touches = _parse_touches(df_sans_joueur, filename)

    # ── Métriques collectives matchs ──────────────────────────────────────────
    df_matchs_stats = _parse_matchs_stats(df_sans_joueur)

    # ── Score et nom complet adversaire ───────────────────────────────────────
    df_score = _compute_match_scores(df_raw)
    df_adv   = _compute_adversaire_nom(df_raw, matchs_df)
    for extra in [df_score, df_adv]:
        if extra.empty:
            continue
        extra = extra.dropna(subset=["match_id"])
        if df_matchs_stats.empty:
            df_matchs_stats = extra
        else:
            df_matchs_stats = df_matchs_stats.merge(extra, on="match_id", how="outer")

    return {
        "perf_match_actions": df_perf_actions,
        "touche":             df_touches,
        "matchs_stats":       df_matchs_stats,
    }


# ── Helpers internes ──────────────────────────────────────────────────────────

def _parse_perf_actions(df: pd.DataFrame, joueurs_df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Agrège les actions individuelles par joueur × match."""
    player_cols = [f"player_{i}" for i in range(1, 24)]
    id_cols     = [c for c in df.columns if c not in player_cols]

    # Filtrer les actions REC uniquement
    df = df[df["team"].str.contains("Rennes", na=False)].copy()

    # Melt player_1..23 → format long
    df_long = df.melt(id_vars=id_cols, value_vars=player_cols, value_name="player_name")
    df_long = df_long[df_long["player_name"].notna()].drop(columns=["variable"])
    df_long["player_name"] = df_long["player_name"].replace(PLAYER_ALIASES)

    # Résolution joueur_id
    df_long = df_long.merge(
        joueurs_df[["joueur_id", "nom"]],
        left_on="player_name", right_on="nom", how="left"
    ).drop(columns=["nom"], errors="ignore")

    base = df_long[["match_id", "joueur_id"]].drop_duplicates().copy()
    agg_frames = []

    # Actions standard
    for action_label, col_prefix in ACTIONS_STANDARD.items():
        sub = df_long[df_long["action"] == action_label]
        if sub.empty:
            continue
        counts = sub.groupby(["match_id", "joueur_id"], dropna=False).agg(**{
            f"{col_prefix}_total":   ("action", "count"),
            f"{col_prefix}_positif": ("label_1_value", lambda x: (x == "positif").sum()),
            f"{col_prefix}_negatif": ("label_1_value", lambda x: (x == "negatif").sum()),
            f"{col_prefix}_neutre":  ("label_1_value", lambda x: (x == "neutre").sum()),
        }).reset_index()
        agg_frames.append(counts)

    # Jeu au pied
    jap = df_long[df_long["action"] == "Jeu au pied"].copy()
    if not jap.empty:
        jap["_start_cat"] = jap["coordinate_start_zone"].apply(zone_start_cat)
        jap["_end_cat"]   = jap["coordinate_end_zone"].apply(zone_end_cat)
        jap_agg = jap.groupby(["match_id", "joueur_id"], dropna=False).agg(
            jap_total               =("action",        "count"),
            jap_positif             =("label_1_value", lambda x: (x == "positif").sum()),
            jap_negatif             =("label_1_value", lambda x: (x == "negatif").sum()),
            jap_neutre              =("label_1_value", lambda x: (x == "neutre").sum()),
            jap_depuis_propre_22m   =("_start_cat",   lambda x: (x == "propre_22m").sum()),
            jap_depuis_mi_terrain   =("_start_cat",   lambda x: (x == "mi_terrain").sum()),
            jap_depuis_camp_adverse =("_start_cat",   lambda x: (x == "camp_adverse").sum()),
            jap_en_touche           =("_end_cat",     lambda x: (x == "en_touche").sum()),
            jap_camp_adverse        =("_end_cat",     lambda x: (x == "camp_adverse").sum()),
        ).reset_index()
        agg_frames.append(jap_agg)

    # Temps de jeu
    tdj = df_long[df_long["action"] == "Temps de jeu"].copy()
    if not tdj.empty:
        tdj["_minutes"] = pd.to_numeric(tdj["label_1_value"], errors="coerce")
        agg_frames.append(
            tdj.groupby(["match_id", "joueur_id"], dropna=False)
               .agg(minutes_jouees=("_minutes", "sum")).reset_index()
        )

    # Buteur
    buteur = df_long[df_long["action"] == "Buteur"].copy()
    if not buteur.empty:
        agg_frames.append(
            buteur.groupby(["match_id", "joueur_id"], dropna=False).agg(
                buts_penalite_reussis =("label_1_value", lambda x: (x == "penalite_+").sum()),
                buts_penalite_rates   =("label_1_value", lambda x: (x == "penalite_-").sum()),
                buts_transfo_reussies =("label_1_value", lambda x: (x == "transformation_+").sum()),
                buts_transfo_ratees   =("label_1_value", lambda x: (x == "transformation_-").sum()),
                buts_drop_reussis     =("label_1_value", lambda x: (x == "drop_+").sum()),
                buts_drop_rates       =("label_1_value", lambda x: (x == "drop_-").sum()),
            ).reset_index()
        )

    # Cartons
    carton = df_long[df_long["action"] == "Carton"].copy()
    if not carton.empty:
        agg_frames.append(
            carton.groupby(["match_id", "joueur_id"], dropna=False).agg(
                cartons_jaunes=("label_1_value", lambda x: (x == "yellow").sum()),
                cartons_rouges=("label_1_value", lambda x: (x == "red").sum()),
            ).reset_index()
        )

    df_result = base.copy()
    for frame in agg_frames:
        df_result = df_result.merge(frame, on=["match_id", "joueur_id"], how="left")

    # Source file
    src = df_long.groupby("match_id")["_source_file"].apply(
        lambda x: ", ".join(sorted(x.dropna().unique()))
    ).reset_index().rename(columns={"_source_file": "_source_file_actions"})
    df_result = df_result.merge(src, on="match_id", how="left")

    # Remplir 0 pour les colonnes de comptage
    count_cols = [c for c in df_result.columns
                  if c not in ("match_id", "joueur_id", "minutes_jouees", "_source_file_actions")]
    df_result[count_cols] = df_result[count_cols].fillna(0).astype(int)

    return df_result


def _parse_touches(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Extrait les données de touche (REC + adversaire)."""
    df_t = df[df["action"] == "Touche"].copy()
    if df_t.empty:
        return pd.DataFrame()

    df_t["equipe"] = df_t["team"].apply(
        lambda t: TEAM_FULL_NAMES.get(str(t).strip(), str(t).strip()) if pd.notna(t) else None
    )
    df_t["est_rec"] = (df_t["equipe"] == "REC").astype(int)

    df_t = df_t.rename(columns={
        "label_1_value":           "resultat",
        "label_2_value":           "sortie",
        "label_3_value":           "alignement",
        "coordinate_unique_zone":  "zone",
        "start":                   "start_sec",
    })

    keep = ["match_id", "equipe", "est_rec", "resultat", "alignement", "sortie", "zone", "start_sec"]
    df_t["_source_file"] = filename
    keep.append("_source_file")
    return df_t[[c for c in keep if c in df_t.columns]].copy()


def _parse_matchs_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les métriques collectives (mêlées, rucks, etc.) par match."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["est_rec"] = df["team"].str.contains("Rennes", na=False)

    def agg_melee(sub, sfx):
        return sub.groupby("match_id", dropna=False).agg(**{
            f"melee_total_{sfx}":   ("action", "count"),
            f"melee_positif_{sfx}": ("label_1_value", lambda x: (x == "positif").sum()),
            f"melee_negatif_{sfx}": ("label_1_value", lambda x: (x == "negatif").sum()),
            f"melee_neutre_{sfx}":  ("label_1_value", lambda x: (x == "neutre").sum()),
        }).reset_index()

    frames = []
    mel_rec = df[df["est_rec"]  & (df["action"] == "Mêlée")]
    mel_adv = df[~df["est_rec"] & (df["action"] == "Mêlée")]
    if not mel_rec.empty:
        frames.append(agg_melee(mel_rec, "rec"))
    if not mel_adv.empty:
        frames.append(agg_melee(mel_adv, "adv"))

    # Rucks, lancements, pénalités, turnovers, possessions, ballons perdus, séquences jeu
    ACTION_COLS = {
        "Ruck":              ("ruck_rec",              "ruck_adv"),
        "Lancement touche":  ("lancement_touche_rec",  "lancement_touche_adv"),
        "Lancement mêlée":   ("lancement_melee_rec",   "lancement_melee_adv"),
        "Pénalité":          ("penalite_rec",           "penalite_adv"),
        "Turnover":          ("turnover_rec",           "turnover_adv"),
        "Possession":        ("possession_rec",         "possession_adv"),
        "Ballon perdu":      ("ballon_perdu_rec",       "ballon_perdu_adv"),
    }
    for action_label, (col_rec, col_adv) in ACTION_COLS.items():
        sub = df[df["action"] == action_label]
        if sub.empty:
            continue
        agg = sub.groupby(["match_id", "est_rec"], dropna=False).size().unstack(fill_value=0)
        if True in agg.columns:
            agg = agg.rename(columns={True: col_rec, False: col_adv})
        frames.append(agg.reset_index())

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.merge(f, on="match_id", how="outer")

    return result


def _compute_match_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule score_rec et score_adv depuis les actions de scoring du match."""
    if df.empty or "match_id" not in df.columns:
        return pd.DataFrame()
    valid = df.dropna(subset=["match_id"]).copy()
    if valid.empty:
        return pd.DataFrame()

    result = valid[["match_id"]].drop_duplicates().copy()
    valid["_est_rec"] = valid["team"].str.contains("Rennes", na=False)

    # Essais par équipe
    essais = valid[valid["action"] == "Essai"]
    if not essais.empty:
        e = essais.groupby(["match_id", "_est_rec"]).size().unstack(fill_value=0).reset_index()
        if True in e.columns:
            result = result.merge(e[["match_id", True]].rename(columns={True: "_e_rec"}), on="match_id", how="left")
        if False in e.columns:
            result = result.merge(e[["match_id", False]].rename(columns={False: "_e_adv"}), on="match_id", how="left")

    # Buts (Buteur) par équipe
    buteur = valid[valid["action"] == "Buteur"]
    for sfx, is_rec in [("rec", True), ("adv", False)]:
        sub = buteur[buteur["_est_rec"] == is_rec]
        if sub.empty:
            continue
        b = sub.groupby("match_id").agg(**{
            f"_t_{sfx}": ("label_1_value", lambda x: (x == "transformation_+").sum()),
            f"_p_{sfx}": ("label_1_value", lambda x: (x == "penalite_+").sum()),
            f"_d_{sfx}": ("label_1_value", lambda x: (x == "drop_+").sum()),
        }).reset_index()
        result = result.merge(b, on="match_id", how="left")

    # Garantit que toutes les colonnes intermédiaires existent
    for col in ["_e_rec", "_e_adv", "_t_rec", "_t_adv", "_p_rec", "_p_adv", "_d_rec", "_d_adv"]:
        if col not in result.columns:
            result[col] = 0
    result = result.fillna(0)

    result["score_rec"] = (result["_e_rec"] * 5 + result["_t_rec"] * 2 + result["_p_rec"] * 3 + result["_d_rec"] * 3).astype(int)
    result["score_adv"] = (result["_e_adv"] * 5 + result["_t_adv"] * 2 + result["_p_adv"] * 3 + result["_d_adv"] * 3).astype(int)

    drop_cols = [c for c in result.columns if c.startswith("_")]
    return result.drop(columns=drop_cols)[["match_id", "score_rec", "score_adv"]]


def _compute_adversaire_nom(df: pd.DataFrame, matchs_df: pd.DataFrame) -> pd.DataFrame:
    """Résout le nom complet de l'adversaire depuis le session_title du match."""
    if df.empty or "match_id" not in df.columns or matchs_df.empty:
        return pd.DataFrame()
    match_ids = df[["match_id"]].dropna(subset=["match_id"]).drop_duplicates()
    result = match_ids.merge(matchs_df[["match_id", "session_title"]], on="match_id", how="left")

    def _resolve(session_title):
        if pd.isna(session_title):
            return None
        parts = str(session_title).split("-")
        if len(parts) != 2:
            return None
        adv = parts[1] if parts[0] == HOME_TEAM else parts[0]
        return ABBREV_TO_FULL_NAME.get(adv, adv)

    result["adversaire_nom_complet"] = result["session_title"].apply(_resolve)
    return result[["match_id", "adversaire_nom_complet"]]
