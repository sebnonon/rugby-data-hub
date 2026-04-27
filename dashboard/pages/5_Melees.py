"""
Dashboard mêlées — Rugby Data Hub
Analyse des mêlées par match (vue équipe + vue joueur avants).
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
st.set_page_config(page_title="Rugby Data Hub — Mêlées", page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg"), layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #071626; color: #e0f0ff; }
    [data-testid="stSidebar"] { background-color: #0d2240; }
    h1 { color: #ffffff; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #e0e0e0; }
    [data-testid="stMetric"] {
        background-color: #0d2240; border: 1px solid #1a3a5c;
        border-radius: 8px; padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #7ab8d8; font-size: 0.8rem; }
    [data-testid="stMetricValue"] { color: #ffffff; font-weight: 700; }
    [data-testid="stSelectbox"] label { color: #7ab8d8; }
    hr { border-color: #1a3a5c; }
    [data-testid="stExpander"] { background-color: #0d2240; border: 1px solid #1a3a5c; border-radius: 8px; }
    .stCaption { color: #4a7a9b; }
    [data-testid="stDataFrame"] { border: 1px solid #1a3a5c; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0d2240", plot_bgcolor="#0d2240",
    font_color="#c0d8f0",
    xaxis=dict(gridcolor="#1a3a5c"), yaxis=dict(gridcolor="#1a3a5c"),
    height=350, margin=dict(t=20),
)

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    c = get_client()
    res = (
        c.table("melee")
        .select(
            "joueur_id, match_id, scrum_num, mi_temps, "
            "avg_total_impact, impact, scrum_load, time_to_feet, "
            "joueur(nom, poste_principal), match(date, adversaire, session_title)"
        )
        .execute()
    )

    def to_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":       r["joueur_id"],
            "match_id":        r["match_id"],
            "scrum_num":       r["scrum_num"],
            "mi_temps":        r["mi_temps"],
            "nom":             r["joueur"]["nom"] if r["joueur"] else None,
            "poste":           r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":            r["match"]["date"] if r["match"] else None,
            "adversaire":      r["match"]["adversaire"] if r["match"] else None,
            "match_titre":     r["match"]["session_title"] if r["match"] else None,
            "impact_total":    to_float(r["avg_total_impact"]),
            "impact":          to_float(r["impact"]),
            "scrum_load":      to_float(r["scrum_load"]),
            "time_to_feet":    to_float(r["time_to_feet"]),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["date", "match_id", "scrum_num"])
    return df


df_all = load_data()

# ── Titre ─────────────────────────────────────────────────────────────────────
col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.image(str(logo_path), width=140)
with col_titre:
    st.title("Rugby Data Hub — Mêlées")
    st.caption("Données mêlée STATSports · Saison")

# ── Slider période ───────────────────────────────────────────────────────────
if not df_all["date"].isna().all():
    d_min, d_max = df_all["date"].min().date(), df_all["date"].max().date()
    d_from, d_to = st.slider(
        "Période", min_value=d_min, max_value=d_max,
        value=(d_min, d_max), format="DD/MM/YY",
    )
    df_all = df_all[(df_all["date"].dt.date >= d_from) & (df_all["date"].dt.date <= d_to)]

# ── Onglets vue équipe / vue joueur ───────────────────────────────────────────
tab_equipe, tab_joueur = st.tabs(["Vue équipe", "Vue joueur"])

# ══════════════════════════════════════════════════════════════════════════════
# VUE ÉQUIPE
# ══════════════════════════════════════════════════════════════════════════════
with tab_equipe:
    # Agrégation par match (une ligne par mêlée = tous les joueurs, on déduplique)
    df_scrums = (
        df_all.groupby(["match_id", "date", "adversaire", "scrum_num"])
        .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
        .reset_index()
    )
    df_par_match = (
        df_scrums.groupby(["match_id", "date", "adversaire"])
        .agg(
            nb_melees=("scrum_num", "count"),
            impact_moy=("impact_moy", "mean"),
            scrum_load_total=("scrum_load_moy", "sum"),
        )
        .reset_index()
        .sort_values("date")
    )
    df_par_match["label_match"] = (
        df_par_match["date"].dt.strftime("%d/%m") + " · " + df_par_match["adversaire"].fillna("?")
    )

    # KPIs saison
    st.subheader("Saison")
    col1, col2, col3 = st.columns(3)
    col1.metric("Mêlées totales",   f"{df_par_match['nb_melees'].sum():.0f}")
    col2.metric("Impact moy.",      f"{df_par_match['impact_moy'].mean():.1f}" if not df_par_match.empty else "—")
    col3.metric("Charge moy./match",f"{df_par_match['scrum_load_total'].mean():.0f}" if not df_par_match.empty else "—")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Nb mêlées par match")
        fig = px.bar(
            df_par_match, x="label_match", y="nb_melees",
            labels={"label_match": "", "nb_melees": "Nb mêlées"},
            color_discrete_sequence=["#56B4E9"],
        )
        fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Impact moyen par match")
        fig2 = px.bar(
            df_par_match, x="label_match", y="impact_moy",
            labels={"label_match": "", "impact_moy": "Impact moy."},
            color_discrete_sequence=["#E69F00"],
        )
        fig2.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
        st.plotly_chart(fig2, use_container_width=True)

    # Détail par match sélectionné
    st.subheader("Détail d'un match")
    matchs_dispo = df_par_match.sort_values("date", ascending=False)["label_match"].tolist()
    match_sel = st.selectbox("Match", matchs_dispo, key="match_equipe")
    match_id_sel = df_par_match[df_par_match["label_match"] == match_sel]["match_id"].values[0]

    df_match = df_all[df_all["match_id"] == match_id_sel].copy()
    df_timeline = (
        df_match.groupby(["scrum_num", "mi_temps"])
        .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
        .reset_index()
        .sort_values(["mi_temps", "scrum_num"])
    )
    df_timeline["label"] = df_timeline.apply(
        lambda r: f"MT{int(r['mi_temps'])} — M{int(r['scrum_num'])}", axis=1
    )

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        x=df_timeline["label"], y=df_timeline["impact_moy"],
        name="Impact moy.", marker_color="#56B4E9"
    ))
    fig3.add_trace(go.Bar(
        x=df_timeline["label"], y=df_timeline["scrum_load_moy"],
        name="Scrum Load moy.", marker_color="#E69F00"
    ))
    fig3.update_layout(
        barmode="group", xaxis_tickangle=-35,
        legend=dict(orientation="h", y=1.12, font_color="#c0d8f0"),
        xaxis_title="", yaxis_title="",
        **PLOTLY_LAYOUT,
    )
    st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# VUE JOUEUR
# ══════════════════════════════════════════════════════════════════════════════
with tab_joueur:
    joueurs = sorted(df_all["nom"].dropna().unique())
    joueur  = st.selectbox("Joueur (avant uniquement)", joueurs, key="joueur_melee")

    df_j = df_all[df_all["nom"] == joueur].copy()

    if df_j.empty:
        st.info("Aucune donnée de mêlée pour ce joueur.")
    else:
        df_j_match = (
            df_j.groupby(["match_id", "date", "adversaire"])
            .agg(
                nb_melees=("scrum_num", "count"),
                impact_moy=("impact", "mean"),
                scrum_load_total=("scrum_load", "sum"),
            )
            .reset_index()
            .sort_values("date")
        )
        df_j_match["label_match"] = (
            df_j_match["date"].dt.strftime("%d/%m") + " · " + df_j_match["adversaire"].fillna("?")
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Mêlées jouées",    f"{len(df_j)}")
        col2.metric("Impact moy.",      f"{df_j['impact'].mean():.1f}" if df_j["impact"].notna().any() else "—")
        col3.metric("Scrum Load total", f"{df_j['scrum_load'].sum():.0f}" if df_j["scrum_load"].notna().any() else "—")

        st.divider()

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Impact par match")
            fig = px.bar(
                df_j_match, x="label_match", y="impact_moy",
                labels={"label_match": "", "impact_moy": "Impact moy."},
                color_discrete_sequence=["#56B4E9"],
            )
            fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Scrum Load par match")
            fig2 = px.bar(
                df_j_match, x="label_match", y="scrum_load_total",
                labels={"label_match": "", "scrum_load_total": "Scrum Load total"},
                color_discrete_sequence=["#E69F00"],
            )
            fig2.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Détail par match"):
            st.dataframe(
                df_j_match[["label_match", "nb_melees", "impact_moy", "scrum_load_total"]]
                .rename(columns={
                    "label_match":       "Match",
                    "nb_melees":         "Nb mêlées",
                    "impact_moy":        "Impact moy.",
                    "scrum_load_total":  "Scrum Load total",
                }),
                use_container_width=True, hide_index=True,
            )
