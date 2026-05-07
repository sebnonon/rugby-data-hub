"""
Dashboard Suivi entraînement — Rugby Data Hub
Cockpit single-screen pour un joueur : sélecteurs cascadés saison / mois / semaine / jour,
KPIs charge GPS + intensité, et 4 graphiques de suivi de charge.
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
st.set_page_config(
    page_title="Rugby Data Hub — Entraînements",
    page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg") if (Path(__file__).parent.parent / "logo.jpg").exists() else "🏉",
    layout="wide",
)

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; }
    @media (max-width: 1280px) { .block-container { zoom: 0.85; } }
    @media (min-width: 1281px) and (max-width: 1600px) { .block-container { zoom: 0.95; } }
    @media (min-width: 1601px) and (max-width: 1920px) { .block-container { zoom: 1.0; } }
    @media (min-width: 1921px) { .block-container { zoom: 1.1; } }
    [data-testid="stSidebar"] { background-color: #ffffff; }
    h1 { color: #071626; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #1a3a5c; }

    /* Cartes KPI */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #c0d8ea;
        border-radius: 8px;
        padding: 8px 12px;
    }
    [data-testid="stMetricLabel"] {
        color: #2a6080;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        font-weight: 700;
    }
    [data-testid="stMetricValue"] {
        color: #071626;
        font-weight: 800;
        font-size: 1rem !important;
        white-space: nowrap;
    }

    /* En-têtes de section */
    .section-header {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        font-weight: 700;
        padding: 6px 0 6px 12px;
        margin: 12px 0 8px 0;
        border-radius: 4px;
    }
    .section-gps  { color: #0a7ab0; border-left: 3px solid #0a7ab0; background: #0a7ab012; }
    .section-int  { color: #E69F00; border-left: 3px solid #E69F00; background: #E69F0010; }

    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stCaption { color: #5a8aaa; }
    .block-container { padding-top: 3.5rem !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
    font_color="#1a3a5c",
    xaxis=dict(gridcolor="#c0d8ea"), yaxis=dict(gridcolor="#c0d8ea"),
    height=280, margin=dict(t=20, b=30, l=40, r=10),
)

# Palette Okabe-Ito
SESSION_COLORS = {
    "J-2":     "#56B4E9",
    "J-1":     "#E69F00",
    "J+2":     "#009E73",
    "Reprise": "#CC79A7",
}

MOIS_FR = {
    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
    5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
    9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_saison(d):
    return f"{d.year}/{d.year+1}" if d.month >= 8 else f"{d.year-1}/{d.year}"


def fmt(val, decimals=0, suffix=""):
    try:
        if pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        return "—"
    try:
        f = float(val)
        if decimals == 0:
            return f"{int(round(f)):,}".replace(",", " ") + suffix
        return f"{f:.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return "—"


def section(label, css_class):
    st.markdown(f'<div class="section-header {css_class}">{label}</div>', unsafe_allow_html=True)


# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    c = get_client()
    res = (
        c.table("perf_entrainement")
        .select(
            "joueur_id, date, session_type, seance_type, titre, "
            "total_distance, max_speed, sprints, sprint_distance, "
            "high_speed_running_absolute, hsr_per_minute_absolute, "
            "hml_distance, hmld_per_minute, "
            "dynamic_stress_load, metabolic_distance_absolute, "
            "accelerations_absolute, decelerations_absolute, "
            "max_acceleration, acute, chronic, acute_chronic_ratio, "
            "joueur(nom, prenom)"
        )
        .execute()
    )
    rows = []
    for r in res.data:
        j = r.get("joueur") or {}
        rows.append({
            "joueur_id":       r["joueur_id"],
            "nom":             j.get("nom"),
            "prenom":          j.get("prenom"),
            "date":            r["date"],
            "session_type":    r["session_type"],
            "seance_type":     r["seance_type"],
            "titre":           r["titre"],
            "distance":        r["total_distance"],
            "vitesse_max":     r["max_speed"],
            "sprints":         r["sprints"],
            "dist_sprint":     r["sprint_distance"],
            "hsr":             r["high_speed_running_absolute"],
            "hsr_per_min":     r["hsr_per_minute_absolute"],
            "hml":             r["hml_distance"],
            "hml_per_min":     r["hmld_per_minute"],
            "dsl":             r["dynamic_stress_load"],
            "dist_metabolique":r["metabolic_distance_absolute"],
            "accels":          r["accelerations_absolute"],
            "decels":          r["decelerations_absolute"],
            "accel_max":       r["max_acceleration"],
            "acute":           r["acute"],
            "chronic":         r["chronic"],
            "acwr":            r["acute_chronic_ratio"],
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    numeric_cols = [
        "distance", "vitesse_max", "sprints", "dist_sprint",
        "hsr", "hsr_per_min", "hml", "hml_per_min",
        "dsl", "dist_metabolique", "accels", "decels",
        "accel_max", "acute", "chronic", "acwr",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("date")
    return df


df_all = load_data()

# ── En-tête : titre + ligne sélecteurs ────────────────────────────────────────
st.markdown(
    '<p style="font-weight:700; font-size:1.1rem; color:#071626; margin:0; padding:0.25rem 0 0.5rem 0; text-align:center;">Suivi entraînement</p>',
    unsafe_allow_html=True,
)

# Liste joueurs
df_j = (
    df_all[["nom", "prenom"]].drop_duplicates().dropna(subset=["nom"]).sort_values("nom")
)
df_j["label"] = df_j.apply(
    lambda r: f"{r['prenom']} {r['nom']}" if pd.notna(r["prenom"]) else r["nom"], axis=1
)
joueur_labels = df_j["label"].tolist()
label_to_nom = dict(zip(df_j["label"], df_j["nom"]))

c_j, c_s, c_m, c_w = st.columns([2, 1.4, 1.6, 2], gap="small")

with c_j:
    joueur_label = st.selectbox("Joueur", joueur_labels, key="e_joueur")
joueur = label_to_nom[joueur_label] if joueur_labels else None

df_p = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy() if joueur else df_all.iloc[0:0].copy()
df_p["saison"] = df_p["date"].apply(_get_saison) if not df_p.empty else pd.Series(dtype=str)

# Saison
saisons_dispo = sorted(df_p["saison"].unique(), reverse=True) if not df_p.empty else []
with c_s:
    saison_sel = st.selectbox(
        "Saison", ["Toutes"] + saisons_dispo, key=f"e_saison_{joueur}"
    )

df_s = df_p if saison_sel == "Toutes" else df_p[df_p["saison"] == saison_sel]

# Mois (cascade depuis saison)
if not df_s.empty:
    mois_pairs = sorted(
        {(d.year, d.month) for d in df_s["date"]}, reverse=True
    )
    mois_options = [f"{MOIS_FR[m]} {y}" for (y, m) in mois_pairs]
    mois_to_pair = dict(zip(mois_options, mois_pairs))
else:
    mois_options, mois_to_pair = [], {}

with c_m:
    mois_sel = st.selectbox(
        "Mois", ["Tous"] + mois_options, key=f"e_mois_{joueur}_{saison_sel}"
    )

if mois_sel == "Tous":
    df_m = df_s
else:
    y_, m_ = mois_to_pair[mois_sel]
    df_m = df_s[(df_s["date"].dt.year == y_) & (df_s["date"].dt.month == m_)]

# Semaine (cascade depuis mois)
if not df_m.empty:
    df_m = df_m.copy()
    df_m["lundi"] = df_m["date"].dt.to_period("W-SUN").dt.start_time
    lundis = sorted(df_m["lundi"].unique())
    sem_options = []
    sem_to_lundi = {}
    for ld in lundis:
        ts = pd.Timestamp(ld)
        iso_w = ts.isocalendar().week
        lbl = f"Sem. {iso_w} — {ts.strftime('%d/%m')}"
        sem_options.append(lbl)
        sem_to_lundi[lbl] = ts.normalize()
else:
    sem_options, sem_to_lundi = [], {}

with c_w:
    sem_sel = st.selectbox(
        "Semaine", ["Toutes"] + sem_options,
        key=f"e_sem_{joueur}_{saison_sel}_{mois_sel}",
    )

if sem_sel == "Toutes":
    df = df_m
else:
    ld = sem_to_lundi[sem_sel]
    df = df_m[(df_m["lundi"] >= ld) & (df_m["lundi"] < ld + pd.Timedelta(days=7))]

if df.empty:
    st.info("Aucune donnée d'entraînement pour cette sélection.")
    st.stop()

# ── Métriques disponibles ─────────────────────────────────────────────────────
METRIQUES = {
    "Distance (m)":             "distance",
    "Vitesse max (km/h)":       "vitesse_max",
    "Sprints":                  "sprints",
    "HSR (m)":                  "hsr",
    "Charge DSL":               "dsl",
    "ACWR":                     "acwr",
    "Accélérations":            "accels",
    "Décélérations":            "decels",
    "Distance sprint (m)":      "dist_sprint",
    "HSR / min":                "hsr_per_min",
    "HML distance (m)":         "hml",
    "Distance métabolique (m)": "dist_metabolique",
    "Accel. max (m/s²)":        "accel_max",
}
METRIQUES_LABELS = list(METRIQUES.keys())

CHART_DEFAULTS = [
    ("Distance (m)",       "Barres"),
    ("Charge DSL",         "Barres"),
    ("ACWR",               "Courbe"),
    ("Vitesse max (km/h)", "Courbe"),
]

# Lecture session_state avant rendu (valeurs disponibles dès le 1er run grâce aux défauts)
chart_configs = [
    (
        st.session_state.get(f"e_met_{i}",  CHART_DEFAULTS[i][0]),
        st.session_state.get(f"e_type_{i}", CHART_DEFAULTS[i][1]),
    )
    for i in range(4)
]


# ── Rendu générique ───────────────────────────────────────────────────────────
def render_chart(col_lbl: str, chart_type: str) -> None:
    col = METRIQUES[col_lbl]
    df_c = df.dropna(subset=[col, "session_type"])
    is_acwr = col == "acwr"

    fig = go.Figure()


    if chart_type == "Barres":
        for stype, color in SESSION_COLORS.items():
            d = df_c[df_c["session_type"] == stype].sort_values("date")
            if d.empty:
                continue
            fig.add_trace(go.Bar(
                x=d["date"],
                y=d[col],
                name=stype,
                marker_color=color,
            ))
        fig.update_layout(
            barmode="stack",
            xaxis=dict(type="date", tickformat="%d/%m", tickangle=-45, gridcolor="#c0d8ea"),
            legend=dict(orientation="h", y=1.18, font_color="#1a3a5c"),
        )
    else:  # Courbe
        df_sorted = df_c.sort_values("date")
        fig.add_trace(go.Scatter(
            x=df_sorted["date"], y=df_sorted[col],
            mode="lines+markers",
            line=dict(color="#0a7ab0", width=2),
            marker=dict(size=6, color="#0a7ab0"),
            showlegend=False,
        ))

    fig.update_layout(
        title=dict(text=col_lbl, x=0.02, font=dict(size=13, color="#1a3a5c")),
        xaxis_title="", yaxis_title=col_lbl,
        **PLOTLY_LAYOUT,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Rendu 2×2 ─────────────────────────────────────────────────────────────────
for row in range(2):
    c_left, c_right = st.columns(2)
    for col_st, i in zip([c_left, c_right], [row * 2, row * 2 + 1]):
        with col_st:
            render_chart(*chart_configs[i])

# ── Sélecteurs par graphique ──────────────────────────────────────────────────
st.divider()
sel_cols = st.columns(4, gap="small")
for i, (def_met, def_type) in enumerate(CHART_DEFAULTS):
    with sel_cols[i]:
        st.caption(f"Graphique {i + 1}")
        st.selectbox(
            "Métrique", METRIQUES_LABELS,
            index=METRIQUES_LABELS.index(chart_configs[i][0]),
            key=f"e_met_{i}",
            label_visibility="collapsed",
        )
        st.radio(
            "Type", ["Barres", "Courbe"],
            index=["Barres", "Courbe"].index(chart_configs[i][1]),
            horizontal=True, key=f"e_type_{i}",
            label_visibility="collapsed",
        )
