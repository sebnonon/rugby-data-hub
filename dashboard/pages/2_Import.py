"""
Page import données — Rugby Data Hub (démo)
"""

import sys
from pathlib import Path
import streamlit as st
from PIL import Image
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from parsers import PARSERS
from supabase_client import get_client

st.set_page_config(page_title="Rugby Data Hub — Import", page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg") if (Path(__file__).parent.parent / "logo.jpg").exists() else "🏉", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; }
    [data-testid="stSidebar"] { background-color: #ffffff; }
    h1, h2, h3 { color: #1a3a5c; }
    [data-testid="stTextInput"] input { background-color: #ffffff; color: #071626; }
    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stAlert { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.image(str(logo_path), width=140)
with col_titre:
    st.title("Import données")
    st.caption("Données STATSports")

st.divider()

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
for attr, label in [("PLAYER_NAME_COLS", "colonne joueur"), ("DISTANCE_COLS", "colonne distance")]:
    alt_cols = getattr(parser, attr, None)
    if alt_cols and not any(c in df_raw.columns for c in alt_cols):
        cols_manquantes.append(f"{label} ({' ou '.join(alt_cols)})")
if cols_manquantes:
    st.error(f"Colonnes requises manquantes : {', '.join(cols_manquantes)}")
    st.stop()

st.markdown(f"**{len(df_raw)} lignes brutes** détectées dans le fichier.")

# ── Chargement des tables de référence depuis Supabase ───────────────────────
@st.cache_data(ttl=120)
def load_refs():
    c = get_client()
    joueurs = pd.DataFrame(c.table("joueur").select("joueur_id, nom").execute().data)
    matchs  = pd.DataFrame(c.table("match").select("match_id, session_title").execute().data)
    return joueurs, matchs

with st.spinner("Chargement des références…"):
    joueurs_df, matchs_df = load_refs()

# ── Parsing ───────────────────────────────────────────────────────────────────
with st.spinner("Parsing du fichier…"):
    try:
        if type_fichier == "actions_match":
            parsed = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
            df_preview = parsed["perf_match_actions"]
        else:
            df_preview = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
    except Exception as e:
        st.error(f"Erreur lors du parsing : {e}")
        st.stop()

# ── Rapport pré-import ────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
lignes_total = len(df_preview)
sans_joueur  = df_preview["joueur_id"].isna().sum() if "joueur_id" in df_preview.columns else 0
sans_match   = df_preview["match_id"].isna().sum()  if "match_id"  in df_preview.columns else 0

col1.metric("Lignes parsées",    lignes_total)
col2.metric("Sans joueur_id ⚠️", sans_joueur)
col3.metric("Sans match_id ⚠️",  sans_match)

with st.expander("Aperçu des données parsées (5 premières lignes)"):
    st.dataframe(df_preview.head(), use_container_width=True, hide_index=True)

# ── Bouton import (démo) ──────────────────────────────────────────────────────
st.divider()

if st.button("Importer dans Supabase", type="primary"):
    st.info("Ceci est une démo — l'import n'est pas réellement disponible.")
