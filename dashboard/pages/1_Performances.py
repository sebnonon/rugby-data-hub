"""
Dashboard Performances — Rugby Data Hub
Outil de pilotage joueur : stats brutes d'un match sélectionné et moyennes saison ramenées à 80 min.
"""

import sys
from pathlib import Path
import streamlit as st
from PIL import Image
import pandas as pd
import plotly.graph_objects as go

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Rugby Data Hub — Performances",
    page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg") if (Path(__file__).parent.parent / "logo.jpg").exists() else "🏉",
    layout="wide",
)

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; }
    [data-testid="stSidebar"] { background-color: #ffffff; }
    h1 { color: #071626; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #1a3a5c; }

    /* Cartes KPI */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #c0d8ea;
        border-radius: 10px;
        padding: 18px 20px;
    }
    [data-testid="stMetricLabel"] {
        color: #2a6080;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        font-weight: 700;
    }
    [data-testid="stMetricValue"] {
        color: #071626;
        font-weight: 800;
        font-size: 1.65rem !important;
        white-space: nowrap;
    }

    /* En-têtes de section */
    .section-header {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        font-weight: 700;
        padding: 6px 0 6px 12px;
        margin: 20px 0 10px 0;
        border-radius: 4px;
    }
    .section-gps  { color: #0a7ab0; border-left: 3px solid #0a7ab0; background: #0a7ab012; }
    .section-tech { color: #E69F00; border-left: 3px solid #E69F00; background: #E69F0010; }

    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stCaption { color: #5a8aaa; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
    .block-container { padding-top: 1.5rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    c = get_client()
    res = (
        c.table("perf_match")
        .select(
            "joueur_id, match_id, "
            "total_distance, max_speed, sprints, high_speed_running_absolute, "
            "dynamic_stress_load, collision_load, "
            "passes_total, passes_positif, "
            "plaquages_total, plaquages_positif, "
            "porteur_total, soutiens_total, contacts_total, essais_total, "
            "minutes_jouees, "
            "joueur(nom, poste_principal), match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, session_title)"
        )
        .execute()
    )

    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":         r["joueur_id"],
            "match_id":          r["match_id"],
            "nom":               r["joueur"]["nom"] if r["joueur"] else None,
            "poste":             r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":              r["match"]["date"] if r["match"] else None,
            "adversaire":        r["match"]["adversaire"] if r["match"] else None,
            "adversaire_complet": r["match"]["adversaire_nom_complet"] if r["match"] else None,
            "score_rec":         r["match"]["score_rec"] if r["match"] else None,
            "score_adv":         r["match"]["score_adv"] if r["match"] else None,
            "match_titre":       r["match"]["session_title"] if r["match"] else None,
            "distance":          r["total_distance"],
            "vitesse_max":       r["max_speed"],
            "sprints":           r["sprints"],
            "hsr":               r["high_speed_running_absolute"],
            "dsl":               r["dynamic_stress_load"],
            "collision_load":    r["collision_load"],
            "passes_total":      r["passes_total"],
            "passes_positif":    r["passes_positif"],
            "plaquages_total":   r["plaquages_total"],
            "plaquages_positif": r["plaquages_positif"],
            "porteur_total":     r["porteur_total"],
            "soutiens_total":    r["soutiens_total"],
            "contacts_total":    r["contacts_total"],
            "essais_total":      r["essais_total"],
            "minutes_jouees":    r["minutes_jouees"],
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt(val, decimals=0, suffix=""):
    """Formate une valeur numérique ; retourne '—' si absente."""
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


def pct_label(total, positif, decimals=0):
    """Construit '14 (86% ✓)' ou '14' si la colonne positif est absente."""
    val_str = fmt(total, decimals=decimals)
    if val_str == "—":
        return "—"
    try:
        if pd.isna(positif):
            return val_str
        t, p = float(total), float(positif)
        if t > 0:
            pct = round(p / t * 100)
            return f"{val_str} ({pct}% ✓)"
    except (TypeError, ValueError):
        pass
    return val_str


# ── Métriques disponibles — GPS et Technique séparés ─────────────────────────
GPS_LABELS: dict[str, str] = {
    "Distance":       "distance",
    "Vitesse max":    "vitesse_max",
    "Sprints":        "sprints",
    "HSR":            "hsr",
    "Charge DSL":     "dsl",
    "Chg. collision": "collision_load",
}

TECH_LABELS: dict[str, str] = {
    "Plaquages":       "plaquages_total",
    "Passes":          "passes_total",
    "Porteurs":        "porteur_total",
    "Soutiens":        "soutiens_total",
    "Contacts":        "contacts_total",
    "Essais":          "essais_total",
    "Plaqs réussies":  "plaquages_positif",
    "Passes réussies": "passes_positif",
}

# Combiné pour le radar (GPS en premier, puis Tech)
RADAR_LABELS: dict[str, str] = {**GPS_LABELS, **TECH_LABELS}

GPS_DEFAULTS:  list[str] = list(GPS_LABELS.keys())
TECH_DEFAULTS: list[str] = ["Plaquages", "Passes", "Porteurs", "Soutiens", "Contacts", "Essais"]

# Métadonnées de rendu par colonne (format, unité, colonne % réussite)
METRIC_META: dict[str, dict] = {
    "distance":          {"label": "Distance",        "suffix": " m",    "decimals": 0},
    "vitesse_max":       {"label": "Vitesse max",     "suffix": " km/h", "decimals": 1},
    "sprints":           {"label": "Sprints",         "suffix": "",      "decimals": 0},
    "hsr":               {"label": "HSR",             "suffix": " m",    "decimals": 0},
    "dsl":               {"label": "Charge DSL",      "suffix": "",      "decimals": 1},
    "collision_load":    {"label": "Chg. collision",  "suffix": "",      "decimals": 1},
    "plaquages_total":   {"label": "Plaquages",       "suffix": "",      "decimals": 0, "pct_col": "plaquages_positif"},
    "passes_total":      {"label": "Passes",          "suffix": "",      "decimals": 0, "pct_col": "passes_positif"},
    "porteur_total":     {"label": "Porteurs",        "suffix": "",      "decimals": 0},
    "soutiens_total":    {"label": "Soutiens",        "suffix": "",      "decimals": 0},
    "contacts_total":    {"label": "Contacts",        "suffix": "",      "decimals": 0},
    "essais_total":      {"label": "Essais",          "suffix": "",      "decimals": 0},
    "plaquages_positif": {"label": "Plaqs réussies",  "suffix": "",      "decimals": 0},
    "passes_positif":    {"label": "Passes réussies", "suffix": "",      "decimals": 0},
}

# Colonnes normalisables par 80 min (Vmax exclue — on prend le max saison)
_PER_80_COLS = [
    "distance", "sprints", "hsr", "dsl", "collision_load",
    "passes_total", "passes_positif",
    "plaquages_total", "plaquages_positif",
    "porteur_total", "soutiens_total", "contacts_total", "essais_total",
]


def compute_moyenne(df_joueur: pd.DataFrame) -> dict:
    """
    Normalise chaque match à 80 min (si minutes_jouees disponible),
    puis calcule la moyenne sur l'ensemble de la saison.
    """
    df_valid = df_joueur[
        df_joueur["minutes_jouees"].notna() & (df_joueur["minutes_jouees"] > 0)
    ].copy()

    has_minutes = len(df_valid) > 0
    df_base = df_valid if has_minutes else df_joueur

    if has_minutes:
        factor = 80 / df_valid["minutes_jouees"]
        for col in _PER_80_COLS:
            if col in df_valid.columns:
                df_valid[col] = df_valid[col] * factor
        df_base = df_valid

    result = {col: df_base[col].mean() for col in _PER_80_COLS if col in df_base.columns}
    result["vitesse_max"] = df_joueur["vitesse_max"].max()
    result["n_matchs"] = len(df_base)
    result["has_minutes"] = has_minutes
    return result


def render_radar(vals: dict, selected_labels: list, team_ref: pd.Series):
    if len(selected_labels) < 3:
        st.info("Sélectionne au moins 3 métriques pour afficher le radar.")
        return

    cols_r = [RADAR_LABELS[lbl] for lbl in selected_labels]
    vals_norm, vals_txt = [], []
    for col in cols_r:
        raw = vals.get(col)
        ref = float(team_ref.get(col, 0) or 0)
        try:
            if pd.isna(raw) or ref == 0:
                vals_norm.append(0)
                vals_txt.append("—")
            else:
                vals_norm.append(min(round(float(raw) / ref * 100, 1), 110))
                vals_txt.append(f"{float(raw):.1f}")
        except (TypeError, ValueError):
            vals_norm.append(0)
            vals_txt.append("—")

    theta = selected_labels + [selected_labels[0]]
    r_plot = vals_norm + [vals_norm[0]]
    t_plot = vals_txt  + [vals_txt[0]]

    fig = go.Figure(go.Scatterpolar(
        r=r_plot, theta=theta,
        fill="toself",
        fillcolor="rgba(10,122,176,0.10)",
        line=dict(color="#0a7ab0", width=2.5),
        text=t_plot,
        hovertemplate="%{theta} : %{text}<extra></extra>",
    ))
    fig.update_layout(
        polar=dict(
            bgcolor="#ffffff",
            radialaxis=dict(
                visible=True, range=[0, 100],
                tickvals=[25, 50, 75, 100],
                tickfont=dict(color="#8ab4c8", size=9),
                gridcolor="#c0d8ea", linecolor="#c0d8ea",
            ),
            angularaxis=dict(
                tickfont=dict(color="#cccccc", size=11),
                gridcolor="#c0d8ea", linecolor="#333",
            ),
        ),
        paper_bgcolor="#f0f6fb",
        font_color="#1a3a5c",
        height=500,
        margin=dict(t=30, b=30, l=80, r=80),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Normalisé sur 100 — 100 = meilleure performance de l'équipe sur la saison")


def section(label, css_class):
    st.markdown(f'<div class="section-header {css_class}">{label}</div>', unsafe_allow_html=True)


def render_kpis_section(
    row: dict,
    selected_labels: list[str],
    section_label: str,
    css_class: str,
    is_moyenne: bool = False,
    all_labels: dict[str, str] = RADAR_LABELS,
):
    """Affiche les cartes KPI pour les métriques sélectionnées, 3 par ligne."""
    section(section_label, css_class)
    if not selected_labels:
        st.caption("Aucune métrique sélectionnée.")
        return
    for i in range(0, len(selected_labels), 3):
        chunk = selected_labels[i:i+3]
        cols = st.columns(3)
        for j, lbl in enumerate(chunk):
            col_name = all_labels.get(lbl)
            if col_name is None:
                continue
            meta = METRIC_META.get(col_name, {"label": lbl, "suffix": "", "decimals": 0})
            d = 1 if is_moyenne else meta.get("decimals", 0)
            if "pct_col" in meta:
                value = pct_label(row.get(col_name), row.get(meta["pct_col"]), decimals=d)
            else:
                value = fmt(row.get(col_name), decimals=d, suffix=meta.get("suffix", ""))
            cols[j].metric(meta["label"], value)


# ── En-tête et sélecteur joueur ───────────────────────────────────────────────
df_all = load_data()

# Référence équipe pour normalisation radar (max de chaque métrique sur la saison)
_radar_cols = [c for c in RADAR_LABELS.values() if c in df_all.columns]
team_max = df_all[_radar_cols].max()

col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.image(str(logo_path), width=80)
with col_titre:
    st.title("Rugby Data Hub — Performances")
    st.caption("Données GPS STATSports · Nationale 1")

joueurs = sorted(df_all["nom"].dropna().unique())

# ── Barre de sélection unique ─────────────────────────────────────────────────
col_joueur, col_match, col_vue = st.columns([2, 4, 2])

with col_joueur:
    joueur = st.selectbox("Joueur", joueurs)

df_joueur = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy()


def _make_match_label(row):
    date_str = row["date"].strftime("%d/%m/%Y")
    nom_adv = row.get("adversaire_complet") or row.get("adversaire") or "?"
    try:
        sr, sa = row.get("score_rec"), row.get("score_adv")
        score_str = f" · {int(sr)}-{int(sa)}" if (pd.notna(sr) and pd.notna(sa)) else ""
    except (TypeError, ValueError):
        score_str = ""
    return f"{date_str} · vs {nom_adv}{score_str}"


df_joueur["label_match"] = df_joueur.apply(_make_match_label, axis=1)
matchs_labels = df_joueur.sort_values("date", ascending=False)["label_match"].tolist()

with col_match:
    match_sel = st.selectbox(
        "Match", matchs_labels if matchs_labels else ["—"],
        key="match_sel", disabled=not matchs_labels,
    )

with col_vue:
    st.markdown('<div style="height:27px"></div>', unsafe_allow_html=True)
    vue = st.radio(
        "Vue", ["🏉 Match", "📊 Moyenne"],
        horizontal=True, label_visibility="collapsed",
    )

st.divider()

# ── Vue Match ─────────────────────────────────────────────────────────────────
if vue == "🏉 Match":
    if df_joueur.empty or not matchs_labels:
        st.info("Aucune donnée disponible pour ce joueur.")
    else:
        row = df_joueur[df_joueur["label_match"] == match_sel].iloc[0].to_dict()
        minutes = row.get("minutes_jouees")
        try:
            st.caption(f"{int(minutes)} min joués" if not pd.isna(minutes) else "Minutes jouées non disponibles")
        except (TypeError, ValueError):
            st.caption("Minutes jouées non disponibles")

        cg, ct = st.columns(2)
        with cg:
            gps_sel_match = st.multiselect(
                "Métriques GPS", list(GPS_LABELS.keys()),
                default=GPS_DEFAULTS, key="gps_match",
            )
        with ct:
            tech_sel_match = st.multiselect(
                "Métriques Technique", list(TECH_LABELS.keys()),
                default=TECH_DEFAULTS, key="tech_match",
            )

        render_kpis_section(row, gps_sel_match,  "GPS",       "section-gps",  all_labels=GPS_LABELS)
        render_kpis_section(row, tech_sel_match, "Technique", "section-tech", all_labels=TECH_LABELS)

        st.divider()
        render_radar(row, gps_sel_match + tech_sel_match, team_max)

# ── Vue Moyenne ───────────────────────────────────────────────────────────────
else:
    if df_joueur.empty:
        st.info("Aucune donnée disponible pour ce joueur.")
    else:
        stats = compute_moyenne(df_joueur)
        n = stats["n_matchs"]
        if stats["has_minutes"]:
            st.caption(f"Moyennes ramenées à 80 min · {n} match{'s' if n > 1 else ''}")
        else:
            st.caption(f"Moyennes brutes · {n} match{'s' if n > 1 else ''} (minutes jouées non disponibles)")

        cg, ct = st.columns(2)
        with cg:
            gps_sel_moy = st.multiselect(
                "Métriques GPS", list(GPS_LABELS.keys()),
                default=GPS_DEFAULTS, key="gps_moy",
            )
        with ct:
            tech_sel_moy = st.multiselect(
                "Métriques Technique", list(TECH_LABELS.keys()),
                default=TECH_DEFAULTS, key="tech_moy",
            )

        render_kpis_section(stats, gps_sel_moy,  "GPS",       "section-gps",  is_moyenne=True, all_labels=GPS_LABELS)
        render_kpis_section(stats, tech_sel_moy, "Technique", "section-tech", is_moyenne=True, all_labels=TECH_LABELS)

        st.divider()
        render_radar(stats, gps_sel_moy + tech_sel_moy, team_max)
