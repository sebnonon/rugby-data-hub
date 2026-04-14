"""
Dashboard entraînements — Rugby Data Hub
Charge GPS par joueur sur la saison (toutes séances confondues).
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Rugby Data Hub — Entraînements", page_icon="🏋️", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0e0e0e; color: #f0f0f0; }
    [data-testid="stSidebar"] { background-color: #1a1a1a; }
    h1 { color: #ffffff; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #e0e0e0; }
    [data-testid="stMetric"] {
        background-color: #1a1a1a; border: 1px solid #333;
        border-radius: 8px; padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #aaaaaa; font-size: 0.8rem; }
    [data-testid="stMetricValue"] { color: #ffffff; font-weight: 700; }
    [data-testid="stSelectbox"] label { color: #aaaaaa; }
    hr { border-color: #333; }
    [data-testid="stExpander"] { background-color: #1a1a1a; border: 1px solid #333; border-radius: 8px; }
    .stCaption { color: #777; }
    [data-testid="stDataFrame"] { border: 1px solid #333; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
    font_color="#cccccc",
    xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
    height=350, margin=dict(t=20),
)

SESSION_COLORS = {
    "J-2":    "#ffffff",
    "J-1":    "#aaaaaa",
    "J+2":    "#555555",
    "Reprise":"#888888",
}

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    c = get_client()
    res = (
        c.table("perf_entrainement")
        .select(
            "joueur_id, date, session_type, seance_type, titre, "
            "total_distance, max_speed, sprints, "
            "high_speed_running_absolute, dynamic_stress_load, "
            "accelerations_absolute, acute_chronic_ratio, "
            "joueur(nom)"
        )
        .execute()
    )
    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":    r["joueur_id"],
            "nom":          r["joueur"]["nom"] if r["joueur"] else None,
            "date":         r["date"],
            "session_type": r["session_type"],
            "seance_type":  r["seance_type"],
            "titre":        r["titre"],
            "distance":     r["total_distance"],
            "vitesse_max":  r["max_speed"],
            "sprints":      r["sprints"],
            "hsr":          r["high_speed_running_absolute"],
            "dsl":          r["dynamic_stress_load"],
            "accels":       r["accelerations_absolute"],
            "acwr":         r["acute_chronic_ratio"],
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date")
    return df


df_all = load_data()

# ── Titre ─────────────────────────────────────────────────────────────────────
col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.png"
    if logo_path.exists():
        st.image(str(logo_path), width=90)
with col_titre:
    st.title("Rugby Data Hub — Entraînements")
    st.caption("Charge GPS STATSports · Saison")

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

df = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy()
df["semaine"] = df["date"].dt.to_period("W").dt.start_time

# ── KPIs ──────────────────────────────────────────────────────────────────────
st.subheader("Saison")

df_dist = df.dropna(subset=["distance"])
nb_sessions  = len(df_dist)
dist_moy     = df_dist["distance"].mean() if nb_sessions else 0
dist_totale  = df_dist["distance"].sum()  if nb_sessions else 0
dsl_total    = df_dist["dsl"].sum()       if "dsl" in df_dist.columns and nb_sessions else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Séances",           f"{nb_sessions}")
col2.metric("Distance moy.",     f"{dist_moy:.0f} m" if nb_sessions else "—")
col3.metric("Distance totale",   f"{dist_totale/1000:.1f} km" if nb_sessions else "—")
col4.metric("Charge totale DSL", f"{dsl_total:.0f}" if nb_sessions else "—")

st.divider()

if df_dist.empty:
    st.info("Aucune donnée d'entraînement disponible pour ce joueur.")
    st.stop()

# ── Graphiques ────────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Distance par séance")
    df_dist["couleur"] = df_dist["session_type"].map(SESSION_COLORS).fillna("#666666")
    fig = go.Figure()
    for stype, color in SESSION_COLORS.items():
        d = df_dist[df_dist["session_type"] == stype]
        if d.empty:
            continue
        fig.add_trace(go.Bar(
            x=d["date"].dt.strftime("%d/%m"),
            y=d["distance"],
            name=stype,
            marker_color=color,
        ))
    fig.update_layout(
        barmode="stack",
        xaxis_tickangle=-45,
        legend=dict(orientation="h", y=1.12, font_color="#cccccc"),
        xaxis_title="", yaxis_title="Distance (m)",
        **PLOTLY_LAYOUT,
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Charge hebdomadaire (DSL)")
    df_hebdo = (
        df.groupby("semaine")["dsl"]
        .sum()
        .reset_index()
        .rename(columns={"semaine": "Semaine", "dsl": "DSL"})
    )
    fig2 = px.bar(
        df_hebdo, x="Semaine", y="DSL",
        labels={"Semaine": "", "DSL": "DSL total"},
        color_discrete_sequence=["#ffffff"],
    )
    fig2.update_layout(xaxis_tickangle=-45, **PLOTLY_LAYOUT)
    st.plotly_chart(fig2, use_container_width=True)

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Distance par type de séance")
    df_type = (
        df_dist.groupby("session_type")["distance"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"session_type": "Type", "mean": "Distance moy. (m)", "count": "Nb séances"})
    )
    fig3 = px.bar(
        df_type, x="Type", y="Distance moy. (m)",
        color="Type",
        color_discrete_map={k: v for k, v in SESSION_COLORS.items()},
        labels={"Type": ""},
    )
    fig3.update_layout(showlegend=False, **PLOTLY_LAYOUT)
    st.plotly_chart(fig3, use_container_width=True)

with col_right2:
    st.subheader("Vitesse max par séance")
    df_vmax = df.dropna(subset=["vitesse_max"])
    fig4 = px.scatter(
        df_vmax,
        x="date", y="vitesse_max",
        color="session_type",
        color_discrete_map=SESSION_COLORS,
        labels={"date": "", "vitesse_max": "Vmax (km/h)", "session_type": "Type"},
    )
    fig4.update_traces(marker_size=7)
    fig4.update_layout(
        legend=dict(orientation="h", y=1.12, font_color="#cccccc"),
        **PLOTLY_LAYOUT,
    )
    st.plotly_chart(fig4, use_container_width=True)

# ── Tableau détail ────────────────────────────────────────────────────────────
with st.expander("Détail par séance"):
    cols_show = ["date", "session_type", "seance_type", "distance",
                 "vitesse_max", "sprints", "hsr", "accels", "dsl"]
    cols_avail = [c for c in cols_show if c in df.columns]
    st.dataframe(
        df[cols_avail].rename(columns={
            "date":         "Date",
            "session_type": "Type",
            "seance_type":  "Séance",
            "distance":     "Distance (m)",
            "vitesse_max":  "Vmax (km/h)",
            "sprints":      "Sprints",
            "hsr":          "HSR (m)",
            "accels":       "Accélérations",
            "dsl":          "DSL",
        }).assign(Date=lambda x: x["Date"].dt.strftime("%d/%m/%Y")),
        use_container_width=True,
        hide_index=True,
    )
