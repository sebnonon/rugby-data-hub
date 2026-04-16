"""
Dashboard MVP — Rugby Data Hub
Performances GPS par joueur sur la saison
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
st.set_page_config(
    page_title="Rugby Data Hub — Dashboard",
    page_icon="🏉",
    layout="wide",
)

st.markdown("""
<style>
    /* Fond général */
    .stApp { background-color: #0e0e0e; color: #f0f0f0; }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #1a1a1a; }

    /* Titre principal */
    h1 { color: #ffffff; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #e0e0e0; }

    /* Metrics */
    [data-testid="stMetric"] {
        background-color: #1a1a1a;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #aaaaaa; font-size: 0.8rem; }
    [data-testid="stMetricValue"] { color: #ffffff; font-weight: 700; }

    /* Selectbox */
    [data-testid="stSelectbox"] label { color: #aaaaaa; }

    /* Divider */
    hr { border-color: #333; }

    /* Expander */
    [data-testid="stExpander"] { background-color: #1a1a1a; border: 1px solid #333; border-radius: 8px; }

    /* Caption */
    .stCaption { color: #777; }

    /* Dataframe */
    [data-testid="stDataFrame"] { border: 1px solid #333; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# Palette Okabe-Ito adaptée fond sombre — accessible daltoniens
COLORS = ["#56B4E9", "#E69F00", "#009E73", "#CC79A7"]

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    """Charge perf_match enrichi avec noms joueur et adversaire."""
    c = get_client()

    # perf_match + nom joueur + infos match
    res = (
        c.table("perf_match")
        .select(
            "joueur_id, match_id, total_distance, max_speed, sprints, "
            "high_speed_running_absolute, collision_load, collisions, "
            "dynamic_stress_load, accelerations_absolute, "
            "joueur(nom), match(date, adversaire, session_title)"
        )
        .execute()
    )

    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":    r["joueur_id"],
            "match_id":     r["match_id"],
            "nom":          r["joueur"]["nom"] if r["joueur"] else None,
            "date":         r["match"]["date"] if r["match"] else None,
            "adversaire":   r["match"]["adversaire"] if r["match"] else None,
            "match_titre":  r["match"]["session_title"] if r["match"] else None,
            "distance":     r["total_distance"],
            "vitesse_max":  r["max_speed"],
            "sprints":      r["sprints"],
            "hsr":          r["high_speed_running_absolute"],
            "collision_load": r["collision_load"],
            "nb_collisions":  r["collisions"],
            "dsl":          r["dynamic_stress_load"],
            "accels":       r["accelerations_absolute"],
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
    st.title("Rugby Data Hub — Performances saison")
    st.caption("Données GPS STATSports · Nationale 1")

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
joueur = st.selectbox("Joueur", joueurs)

df = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy()
df["label_match"] = df["date"].dt.strftime("%d/%m") + " · " + df["adversaire"].fillna("?")

# ── KPIs saison ───────────────────────────────────────────────────────────────
st.subheader("Saison")

df_gps = df.dropna(subset=["distance"])  # uniquement matchs avec données GPS

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Matchs joués",     f"{len(df_gps)}")
col2.metric("Distance moy.",    f"{df_gps['distance'].mean():.0f} m" if not df_gps.empty else "—")
col3.metric("Vitesse max",      f"{df_gps['vitesse_max'].max():.1f} km/h" if not df_gps.empty else "—")
col4.metric("Sprints (moy.)",   f"{df_gps['sprints'].mean():.0f}" if not df_gps.empty else "—")
col5.metric("Collisions (moy.)",f"{df['nb_collisions'].mean():.0f}" if not df['nb_collisions'].isna().all() else "—")

st.divider()

# ── Graphiques ────────────────────────────────────────────────────────────────
if df_gps.empty:
    st.info("Aucune donnée GPS disponible pour ce joueur.")
else:
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Distance par match")
        fig = px.bar(
            df_gps,
            x="label_match",
            y="distance",
            labels={"label_match": "", "distance": "Distance (m)"},
            color_discrete_sequence=["#56B4E9"],
        )
        fig.update_layout(
            xaxis_tickangle=-35, height=350, margin=dict(t=20),
            paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
            font_color="#cccccc", xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Vitesse max par match")
        fig2 = px.bar(
            df_gps,
            x="label_match",
            y="vitesse_max",
            labels={"label_match": "", "vitesse_max": "Vitesse max (km/h)"},
            color_discrete_sequence=["#E69F00"],
        )
        fig2.update_layout(
            xaxis_tickangle=-35, height=350, margin=dict(t=20),
            paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
            font_color="#cccccc", xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("Sprints & HSR")
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=df_gps["label_match"], y=df_gps["sprints"],
            name="Sprints", marker_color="#56B4E9"
        ))
        fig3.add_trace(go.Bar(
            x=df_gps["label_match"], y=df_gps["hsr"],
            name="HSR (m)", marker_color="#E69F00"
        ))
        fig3.update_layout(
            barmode="group", xaxis_tickangle=-35,
            height=350, margin=dict(t=20),
            legend=dict(orientation="h", y=1.1, font_color="#cccccc"),
            paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
            font_color="#cccccc", xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_right2:
        st.subheader("Charge de collision")
        df_col = df.dropna(subset=["collision_load"])
        if df_col.empty:
            st.info("Pas de données de collision.")
        else:
            fig4 = px.bar(
                df_col,
                x="label_match",
                y="collision_load",
                labels={"label_match": "", "collision_load": "Collision Load"},
                color_discrete_sequence=["#009E73"],
            )
            fig4.update_layout(
                xaxis_tickangle=-35, height=350, margin=dict(t=20),
                paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
                font_color="#cccccc", xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
            )
            st.plotly_chart(fig4, use_container_width=True)

# ── Tableau détail ────────────────────────────────────────────────────────────
with st.expander("Détail par match"):
    cols_show = ["label_match", "distance", "vitesse_max", "sprints", "hsr",
                 "nb_collisions", "collision_load", "accels", "dsl"]
    cols_avail = [c for c in cols_show if c in df.columns]
    st.dataframe(
        df[cols_avail].rename(columns={
            "label_match":    "Match",
            "distance":       "Distance (m)",
            "vitesse_max":    "Vmax (km/h)",
            "sprints":        "Sprints",
            "hsr":            "HSR (m)",
            "nb_collisions":  "Collisions",
            "collision_load": "Col. Load",
            "accels":         "Accélérations",
            "dsl":            "DSL",
        }),
        use_container_width=True,
        hide_index=True,
    )
