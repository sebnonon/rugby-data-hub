"""
Page import données — Rugby Data Hub (staff uniquement)
Permet d'uploader des fichiers CSV STATSports et de les injecter dans Supabase.
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from auth import require_auth
require_auth()

import sys
from pathlib import Path
import streamlit as st
import pandas as pd

# Accès aux parsers depuis le dossier pipeline
sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from parsers import PARSERS
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Rugby Data Hub — Import", page_icon="📤", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0e0e0e; color: #f0f0f0; }
    [data-testid="stSidebar"] { background-color: #1a1a1a; }
    h1, h2, h3 { color: #e0e0e0; }
    [data-testid="stTextInput"] input { background-color: #1a1a1a; color: #f0f0f0; }
    [data-testid="stSelectbox"] label { color: #aaaaaa; }
    hr { border-color: #333; }
    .stAlert { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── Authentification staff ────────────────────────────────────────────────────
col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.png"
    if logo_path.exists():
        st.image(str(logo_path), width=90)
with col_titre:
    st.title("Import données — Staff")
    st.caption("Accès restreint · Données STATSports")

st.divider()

mot_de_passe = st.text_input("Mot de passe staff", type="password")

try:
    mdp_attendu = st.secrets["STAFF_PASSWORD"]
except Exception:
    mdp_attendu = None

if not mot_de_passe:
    st.info("Entrez le mot de passe pour accéder à l'import.")
    st.stop()

if mot_de_passe != mdp_attendu:
    st.error("Mot de passe incorrect.")
    st.stop()


# ── Suppression d'un import ───────────────────────────────────────────────────
with st.expander("Supprimer un import existant"):
    @st.cache_data(ttl=30)
    def list_imported_files():
        c = get_client()
        fichiers = {}

        def fetch_distinct(table, col):
            try:
                rows = c.table(table).select(col).execute().data
                return sorted({r[col] for r in rows if r.get(col)})
            except Exception:
                return []

        fichiers["perf_match (GPS)"]     = fetch_distinct("perf_match", "_source_file_gps")
        fichiers["perf_match (actions)"] = fetch_distinct("perf_match", "_source_file_actions")
        fichiers["perf_entrainement"]    = fetch_distinct("perf_entrainement", "_source_file")
        fichiers["collision"]            = fetch_distinct("collision", "_source_file")
        fichiers["melee"]                = fetch_distinct("melee", "_source_file")
        fichiers["touche"]               = fetch_distinct("touche", "_source_file")
        return fichiers

    fichiers_par_table = list_imported_files()
    options = [(t, f) for t, flist in fichiers_par_table.items() for f in flist]

    if not options:
        st.info("Aucun fichier importé pour l'instant.")
    else:
        labels = [f"{f}  ·  {t}" for t, f in options]
        choix  = st.selectbox("Fichier à supprimer", labels, key="del_fichier")
        idx    = labels.index(choix)
        table_cible, fichier_cible = options[idx]

        col_source = (
            "_source_file_gps"     if table_cible == "perf_match (GPS)"
            else "_source_file_actions" if table_cible == "perf_match (actions)"
            else "_source_file"
        )
        table_sql = "perf_match" if "perf_match" in table_cible else table_cible

        st.warning(
            f"Supprimera toutes les lignes de **`{table_sql}`** "
            f"où `{col_source}` = `{fichier_cible}`"
        )
        confirm = st.checkbox("Je confirme la suppression", key="del_confirm")
        if st.button("Supprimer", type="primary", disabled=not confirm, key="del_btn"):
            try:
                c = get_client()
                c.table(table_sql).delete().eq(col_source, fichier_cible).execute()
                st.success(f"Lignes supprimées dans `{table_sql}` pour `{fichier_cible}`.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Erreur : {e}")

st.divider()

# ── Interface d'import ────────────────────────────────────────────────────────
LABELS = {
    "gps_match":        "GPS Match",
    "gps_entrainement": "GPS Entraînement",
    "gps_collision":    "GPS Collision",
    "gps_melee":        "GPS Mêlée",
    "actions_match":    "Actions Match (codage vidéo)",
}

NOMENCLATURE_AIDE = {
    "gps_match":        "gps_match_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_match_REC-OMR_2025-11-15.csv",
    "gps_entrainement": "gps_entrainement_YYYY-MM-DD_(Reprise|J-2|J-1|J+2).csv\nEx : gps_entrainement_2025-11-14_J-2.csv",
    "gps_collision":    "gps_collision_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_collision_REC-NRC_2025-09-13.csv",
    "gps_melee":        "gps_melee_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_melee_REC-CSBJ_2025-11-08.csv",
    "actions_match":    "actions_match_EQ1-EQ2_YYYY-MM-DD.csv\nEx : actions_match_REC-OMR_2025-11-15.csv",
}

TABLES_CIBLES = {
    "gps_match":        ["perf_match (colonnes GPS)"],
    "gps_entrainement": ["perf_entrainement"],
    "gps_collision":    ["collision"],
    "gps_melee":        ["melee"],
    "actions_match":    ["perf_match (colonnes actions)", "touche", "match (métriques collectives)"],
}

col_sel, col_info = st.columns([2, 3])

with col_sel:
    type_fichier = st.selectbox(
        "Type de fichier",
        list(PARSERS.keys()),
        format_func=lambda k: LABELS[k],
    )

with col_info:
    st.markdown("**Nomenclature attendue :**")
    st.code(NOMENCLATURE_AIDE[type_fichier], language=None)
    st.markdown(f"**Tables alimentées :** {', '.join(TABLES_CIBLES[type_fichier])}")

st.divider()

uploaded = st.file_uploader("Choisir un fichier CSV", type=["csv"])

if uploaded is None:
    st.stop()

# ── Validation du nom de fichier ──────────────────────────────────────────────
parser = PARSERS[type_fichier]
valide, erreur, metadata = parser.validate_filename(uploaded.name)

if not valide:
    st.error(f"**Nom de fichier invalide**\n\n{erreur}")
    st.stop()

st.success(f"Nomenclature valide : **{uploaded.name}**")
if metadata:
    meta_str = " · ".join(f"{k} = `{v}`" for k, v in metadata.items())
    st.caption(f"Métadonnées extraites : {meta_str}")

# ── Chargement et validation du contenu ──────────────────────────────────────
try:
    df_raw = pd.read_csv(uploaded)
except Exception as e:
    st.error(f"Impossible de lire le CSV : {e}")
    st.stop()

cols_manquantes = [c for c in parser.REQUIRED_COLUMNS if c not in df_raw.columns]
# Colonnes alternatives (joueur, distance…) : au moins une variante doit être présente
for attr, label in [("PLAYER_NAME_COLS", "colonne joueur"), ("DISTANCE_COLS", "colonne distance")]:
    alt_cols = getattr(parser, attr, None)
    if alt_cols and not any(c in df_raw.columns for c in alt_cols):
        cols_manquantes.append(f"{label} ({' ou '.join(alt_cols)})")
if cols_manquantes:
    st.error(f"Colonnes requises manquantes : {', '.join(cols_manquantes)}")
    st.stop()

st.markdown(f"**{len(df_raw)} lignes brutes** détectées dans le fichier.")

# ── Fonctions d'upsert Supabase ───────────────────────────────────────────────

import numpy as np
import math

def _clean_records(df: pd.DataFrame) -> list[dict]:
    """Convertit un DataFrame en liste de dicts JSON-compatibles.
    - nan/inf → None
    - numpy int/float → types Python natifs
    - float entier (1.0) → int (pour les colonnes INTEGER Supabase)
    """
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if v is pd.NA or v is pd.NaT:
                clean[k] = None
            elif isinstance(v, np.integer):
                clean[k] = int(v)
            elif isinstance(v, (np.floating, float)):
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    clean[k] = None
                elif fv == int(fv):
                    clean[k] = int(fv)
                else:
                    clean[k] = fv
            else:
                clean[k] = v
        records.append(clean)
    return records


# Colonnes PK auto-générées par Supabase — à exclure de tout insert
_PK_COLS = {
    "perf_match":        "perf_match_id",
    "perf_entrainement": "perf_entr_id",
    "collision":         "collision_id",
    "melee":             "melee_id",
    "touche":            "touche_id",
}


def _drop_pk(df: pd.DataFrame, table: str) -> pd.DataFrame:
    pk = _PK_COLS.get(table)
    if pk and pk in df.columns:
        df = df.drop(columns=[pk])
    return df


def _upsert_table(client, table: str, df: pd.DataFrame,
                  source_col: str, filename: str) -> list[str]:
    """
    Supprime les lignes existantes pour ce fichier source, puis insère les nouvelles.
    Retourne une liste de messages de résultat.
    """
    if df.empty:
        return [f"⚠️ `{table}` : aucune donnée à importer."]
    client.table(table).delete().eq(source_col, filename).execute()
    data = _clean_records(_drop_pk(df, table))
    BATCH = 500
    for i in range(0, len(data), BATCH):
        client.table(table).insert(data[i:i + BATCH]).execute()
    return [f"✅ `{table}` : {len(df)} lignes importées."]


def _dedup_perf_match(df: pd.DataFrame) -> pd.DataFrame:
    """
    Déduplique sur (joueur_id, match_id) en agrégeant les lignes multiples
    (ex. deux mi-temps dans le même CSV) : somme pour les métriques cumulatives,
    max pour les pics (vitesse, accélération).
    Les colonnes non numériques (total_time, _source_file_*) gardent la dernière valeur.
    """
    if "joueur_id" not in df.columns or "match_id" not in df.columns:
        return df
    if not df.duplicated(subset=["joueur_id", "match_id"]).any():
        return df

    # Colonnes de regroupement et colonnes texte à conserver telles quelles
    key_cols  = ["joueur_id", "match_id"]
    text_cols = [c for c in df.columns if df[c].dtype == object]
    num_cols  = [c for c in df.columns if c not in key_cols and c not in text_cols]

    peak_cols = [c for c in num_cols if any(
        k in c for k in ("max_speed", "max_accel", "max_decel", "step_balance",
                         "acute", "chronic", "acute_chronic", "average_speed",
                         "average_metabolic", "distance_per_min", "hsr_per_minute",
                         "hmld_per_minute", "accelerations_per_min", "decels_per_min")
    )]
    sum_cols  = [c for c in num_cols if c not in peak_cols]

    agg: dict = {c: "sum" for c in sum_cols}
    agg.update({c: "max" for c in peak_cols})
    agg.update({c: "last" for c in text_cols if c not in key_cols})

    return df.groupby(key_cols, as_index=False).agg(agg)


def _autocreate_joueurs(client, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Crée automatiquement les joueurs manquants (joueur_id NaN) dans la table joueur,
    puis met à jour le joueur_id dans le DataFrame.
    Retourne (df mis à jour, messages de résultat).
    """
    msgs = []
    if "joueur_id" not in df.columns or "player_name" not in df.columns:
        return df, msgs

    noms_manquants = (
        df[df["joueur_id"].isna()]["player_name"]
        .dropna().unique()
    )
    if len(noms_manquants) == 0:
        return df, msgs

    client.table("joueur").insert(
        [{"nom": n} for n in noms_manquants]
    ).execute()

    joueurs_fresh = pd.DataFrame(
        client.table("joueur").select("joueur_id, nom").execute().data
    )
    mapping = joueurs_fresh.set_index("nom")["joueur_id"]
    mask = df["joueur_id"].isna()
    df = df.copy()
    df.loc[mask, "joueur_id"] = df.loc[mask, "player_name"].map(mapping)
    msgs.append(
        f"✅ {len(noms_manquants)} nouveau(x) joueur(s) créé(s) : "
        f"{', '.join(noms_manquants)}"
    )
    return df, msgs


def _upsert_perf_match_gps(client, df: pd.DataFrame, filename: str) -> list[str]:
    """Upsert des colonnes GPS dans perf_match sur (joueur_id, match_id)."""
    if df.empty:
        return ["⚠️ `perf_match` (GPS) : aucune donnée à importer."]
    df = _dedup_perf_match(df)
    data = _clean_records(_drop_pk(df, "perf_match"))
    BATCH = 500
    for i in range(0, len(data), BATCH):
        client.table("perf_match").upsert(
            data[i:i + BATCH], on_conflict="joueur_id,match_id"
        ).execute()
    return [f"✅ `perf_match` (GPS) : {len(df)} lignes importées."]


def _upsert_perf_match_actions(client, df: pd.DataFrame, filename: str) -> list[str]:
    """Upsert des colonnes actions dans perf_match sur (joueur_id, match_id)."""
    if df.empty:
        return ["⚠️ `perf_match` (actions) : aucune donnée à importer."]
    df = _dedup_perf_match(df)
    data = _clean_records(_drop_pk(df, "perf_match"))
    BATCH = 500
    for i in range(0, len(data), BATCH):
        client.table("perf_match").upsert(
            data[i:i + BATCH], on_conflict="joueur_id,match_id"
        ).execute()
    return [f"✅ `perf_match` (actions) : {len(df)} lignes importées."]


def _upsert_matchs_stats(client, df: pd.DataFrame) -> list[str]:
    """Met à jour les métriques collectives dans la table match (upsert par match_id)."""
    if df.empty:
        return []
    data = _clean_records(df)
    client.table("match").upsert(data, on_conflict="match_id").execute()
    return [f"✅ `match` (métriques collectives) : {len(df)} matchs mis à jour."]


# ── Chargement des tables de référence depuis Supabase ───────────────────────
@st.cache_data(ttl=120)
def load_refs():
    c = get_client()
    joueurs = pd.DataFrame(c.table("joueur").select("joueur_id, nom").execute().data)
    matchs  = pd.DataFrame(c.table("match").select("match_id, session_title").execute().data)
    return joueurs, matchs

with st.spinner("Chargement des références Supabase…"):
    joueurs_df, matchs_df = load_refs()

# ── Parsing ───────────────────────────────────────────────────────────────────
with st.spinner("Parsing du fichier…"):
    try:
        if type_fichier == "actions_match":
            parsed = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
            df_preview = parsed["perf_match_actions"]
        else:
            df_preview = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
            parsed = None
    except Exception as e:
        st.error(f"Erreur lors du parsing : {e}")
        st.stop()

# ── Rapport pré-import ────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

lignes_total = len(df_preview)
sans_joueur  = df_preview["joueur_id"].isna().sum() if "joueur_id" in df_preview.columns else 0
sans_match   = df_preview["match_id"].isna().sum()  if "match_id"  in df_preview.columns else 0

col1.metric("Lignes parsées",     lignes_total)
col2.metric("Sans joueur_id ⚠️",  sans_joueur)
col3.metric("Sans match_id ⚠️",   sans_match)

with st.expander("Aperçu des données parsées (5 premières lignes)"):
    st.dataframe(df_preview.head(), use_container_width=True, hide_index=True)

if sans_joueur > 0 and "player_name" in df_preview.columns:
    noms_manquants = sorted(
        df_preview[df_preview["joueur_id"].isna()]["player_name"].dropna().unique()
    )
    st.info(
        f"{sans_joueur} ligne(s) sans `joueur_id`. "
        f"Ces joueurs seront **créés automatiquement** dans la base lors de l'import : "
        f"{', '.join(f'`{n}`' for n in noms_manquants)}"
    )
if sans_match > 0:
    st.warning(
        f"{sans_match} ligne(s) sans `match_id` — vérifiez que le match est bien enregistré."
    )

# ── Confirmation et import ────────────────────────────────────────────────────
st.divider()
st.markdown("**L'import écrasera les données existantes** pour ce fichier source.")

if st.button("Importer dans Supabase", type="primary"):
    with st.spinner("Import en cours…"):
        try:
            c = get_client()
            resultats = []

            if type_fichier == "gps_match":
                df_gps, msgs = _autocreate_joueurs(c, df_preview)
                resultats += msgs
                resultats += _upsert_perf_match_gps(c, df_gps, uploaded.name)

            elif type_fichier == "gps_entrainement":
                df_entr, msgs = _autocreate_joueurs(c, df_preview)
                resultats += msgs
                df_entr = df_entr.drop(columns=["player_name"], errors="ignore")
                resultats += _upsert_table(c, "perf_entrainement", df_entr, "_source_file", uploaded.name)

            elif type_fichier == "gps_collision":
                df_col, msgs = _autocreate_joueurs(c, df_preview)
                resultats += msgs
                resultats += _upsert_table(c, "collision", df_col, "_source_file", uploaded.name)

            elif type_fichier == "gps_melee":
                df_mel, msgs = _autocreate_joueurs(c, df_preview)
                resultats += msgs
                resultats += _upsert_table(c, "melee", df_mel, "_source_file", uploaded.name)

            elif type_fichier == "actions_match":
                df_actions, msgs = _autocreate_joueurs(c, parsed["perf_match_actions"])
                resultats += msgs
                resultats += _upsert_perf_match_actions(c, df_actions, uploaded.name)
                if not parsed["touche"].empty:
                    resultats += _upsert_table(c, "touche", parsed["touche"], "_source_file", uploaded.name)
                if not parsed["matchs_stats"].empty:
                    resultats += _upsert_matchs_stats(c, parsed["matchs_stats"])

            for msg in resultats:
                st.success(msg)

        except Exception as e:
            st.error(f"Erreur lors de l'import : {e}")


