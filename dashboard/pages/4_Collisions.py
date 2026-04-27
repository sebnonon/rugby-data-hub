"""
Dashboard collisions — Rugby Data Hub
Analyse des événements de collision par joueur.
"""

import sys
from pathlib import Path
import streamlit as st
from PIL import Image
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Rugby Data Hub — Collisions", page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg"), layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; }
    [data-testid="stSidebar"] { background-color: #ffffff; }
    h1 { color: #ffffff; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #1a3a5c; }
    [data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #c0d8ea;
        border-radius: 8px; padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #2a6080; font-size: 0.8rem; }
    [data-testid="stMetricValue"] { color: #ffffff; font-weight: 700; }
    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    [data-testid="stExpander"] { background-color: #ffffff; border: 1px solid #c0d8ea; border-radius: 8px; }
    .stCaption { color: #5a8aaa; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
    font_color="#1a3a5c",
    xaxis=dict(gridcolor="#c0d8ea"), yaxis=dict(gridcolor="#c0d8ea"),
    height=350, margin=dict(t=20),
)

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    c = get_client()
    res = (
        c.table("collision")
        .select(
            "joueur_id, match_id, mi_temps, start_time, "
            "collision_load, time_to_feet, post_collision_accel, "
            "joueur(nom), match(date, adversaire, session_title)"
        )
        .execute()
    )
    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":           r["joueur_id"],
            "match_id":            r["match_id"],
            "nom":                 r["joueur"]["nom"] if r["joueur"] else None,
            "date":                r["match"]["date"] if r["match"] else None,
            "adversaire":          r["match"]["adversaire"] if r["match"] else None,
            "match_titre":         r["match"]["session_title"] if r["match"] else None,
            "mi_temps":            r["mi_temps"],
            "start_time":          r["start_time"],
            "collision_load":      r["collision_load"],
            "time_to_feet":        r["time_to_feet"],
            "post_collision_accel":r["post_collision_accel"],
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["date", "match_id"])
    return df


df_all = load_data()

# ── Titre ─────────────────────────────────────────────────────────────────────
col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.image(str(logo_path), width=140)
with col_titre:
    st.title("Rugby Data Hub — Collisions")
    st.caption("Événements de collision STATSports · Saison")

# ── Slider période ───────────────────────────────────────────────────────────
if not df_all["date"].isna().all():
    d_min, d_max = df_all["date"].min().date(), df_all["date"].max().date()
    d_from, d_to = st.slider(
        "Période", min_value=d_min, max_value=d_max,
        value=(d_min, d_max), format="DD/MM/YY",
    )
    df_all = df_all[(df_all["date"].dt.date >= d_from) & (df_all["date"].dt.date <= d_to)]

# ── Sélecteur joueur ──────────────────────────────────────────────────────────
joueurs = sorted(df_all["nom"].dropna().unique())
joueur  = st.selectbox("Joueur", joueurs)

df = df_all[df_all["nom"] == joueur].copy()

# Agrégation par match pour les graphiques
df_par_match = (
    df.groupby(["match_id", "date", "adversaire"])
    .agg(
        nb_collisions=("collision_load", "count"),
        collision_load_total=("collision_load", "sum"),
        collision_load_moy=("collision_load", "mean"),
        time_to_feet_moy=("time_to_feet", "mean"),
    )
    .reset_index()
    .sort_values("date")
)
df_par_match["label_match"] = (
    df_par_match["date"].dt.strftime("%d/%m") + " · " + df_par_match["adversaire"].fillna("?")
)

# ── KPIs ──────────────────────────────────────────────────────────────────────
st.subheader("Saison")

nb_total    = len(df)
load_total  = df["collision_load"].sum()
load_moy    = df["collision_load"].mean()
ttf_moy     = df["time_to_feet"].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Collisions totales",   f"{nb_total}")
col2.metric("Charge totale",        f"{load_total:.0f}" if nb_total else "—")
col3.metric("Charge moy./collision",f"{load_moy:.1f}" if nb_total else "—")
col4.metric("Time to feet moy.",    f"{ttf_moy:.2f} s" if nb_total else "—")

st.divider()

if df.empty:
    st.info("Aucune donnée de collision pour ce joueur.")
    st.stop()

# ── Graphiques ────────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Nb collisions par match")
    fig = px.bar(
        df_par_match,
        x="label_match", y="nb_collisions",
        labels={"label_match": "", "nb_collisions": "Nb collisions"},
        color_discrete_sequence=["#56B4E9"],
    )
    fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Charge de collision par match")
    fig2 = px.bar(
        df_par_match,
        x="label_match", y="collision_load_total",
        labels={"label_match": "", "collision_load_total": "Collision Load total"},
        color_discrete_sequence=["#E69F00"],
    )
    fig2.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
    st.plotly_chart(fig2, use_container_width=True)

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Distribution de la collision load")
    fig3 = px.histogram(
        df, x="collision_load", nbins=30,
        labels={"collision_load": "Collision Load", "count": "Nb événements"},
        color_discrete_sequence=["#56B4E9"],
    )
    fig3.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig3, use_container_width=True)

with col_right2:
    st.subheader("Time to feet par match")
    fig4 = px.scatter(
        df_par_match,
        x="label_match", y="time_to_feet_moy",
        size="nb_collisions",
        labels={"label_match": "", "time_to_feet_moy": "Time to feet moy. (s)", "nb_collisions": "Nb collisions"},
        color_discrete_sequence=["#CC79A7"],
    )
    fig4.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
    st.plotly_chart(fig4, use_container_width=True)

# ── Tableau détail par match ──────────────────────────────────────────────────
with st.expander("Détail par match"):
    st.dataframe(
        df_par_match[["label_match", "nb_collisions", "collision_load_total",
                       "collision_load_moy", "time_to_feet_moy"]]
        .rename(columns={
            "label_match":          "Match",
            "nb_collisions":        "Nb collisions",
            "collision_load_total": "Load total",
            "collision_load_moy":   "Load moy.",
            "time_to_feet_moy":     "Time to feet moy. (s)",
        }),
        use_container_width=True, hide_index=True,
    )
