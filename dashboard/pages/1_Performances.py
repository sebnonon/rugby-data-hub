"""
Dashboard Performances — Rugby Data Hub
Outil de pilotage joueur : stats brutes d'un match sélectionné et moyennes saison ramenées à 80 min.
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Rugby Data Hub — Performances",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
    .stApp { background-color: #0e0e0e; color: #f0f0f0; }
    [data-testid="stSidebar"] { background-color: #1a1a1a; }
    h1 { color: #ffffff; font-weight: 800; letter-spacing: 1px; }
    h2, h3 { color: #e0e0e0; }

    /* Cartes KPI */
    [data-testid="stMetric"] {
        background-color: #161616;
        border: 1px solid #2a2a2a;
        border-radius: 10px;
        padding: 18px 20px;
    }
    [data-testid="stMetricLabel"] {
        color: #888;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        font-weight: 600;
    }
    [data-testid="stMetricValue"] {
        color: #ffffff;
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
    .section-gps  { color: #56B4E9; border-left: 3px solid #56B4E9; background: #56B4E910; }
    .section-tech { color: #E69F00; border-left: 3px solid #E69F00; background: #E69F0010; }

    [data-testid="stSelectbox"] label { color: #aaaaaa; }
    hr { border-color: #2a2a2a; }
    .stCaption { color: #666; }
    [data-testid="stDataFrame"] { border: 1px solid #2a2a2a; border-radius: 8px; }
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
            "joueur(nom, poste_principal), match(date, adversaire, session_title)"
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


# ── Radar — métriques disponibles ────────────────────────────────────────────
# Ordre = ordre d'affichage sur le radar
RADAR_LABELS: dict[str, str] = {
    # GPS
    "Distance":          "distance",
    "Vitesse max":       "vitesse_max",
    "Sprints":           "sprints",
    "HSR":               "hsr",
    "Charge DSL":        "dsl",
    "Chg. collision":    "collision_load",
    # Technique
    "Plaquages":         "plaquages_total",
    "Passes":            "passes_total",
    "Porteurs":          "porteur_total",
    "Soutiens":          "soutiens_total",
    "Contacts":          "contacts_total",
    "Essais":            "essais_total",
    # Extras disponibles mais non affichés par défaut
    "Plaqs réussies":    "plaquages_positif",
    "Passes réussies":   "passes_positif",
}

RADAR_DEFAULTS: list[str] = [
    "Distance", "Vitesse max", "Sprints", "HSR", "Charge DSL", "Chg. collision",
    "Plaquages", "Passes", "Porteurs", "Soutiens", "Contacts", "Essais",
]

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
        fillcolor="rgba(86,180,233,0.09)",
        line=dict(color="#56B4E9", width=2.5),
        text=t_plot,
        hovertemplate="%{theta} : %{text}<extra></extra>",
    ))
    fig.update_layout(
        polar=dict(
            bgcolor="#161616",
            radialaxis=dict(
                visible=True, range=[0, 100],
                tickvals=[25, 50, 75, 100],
                tickfont=dict(color="#444", size=9),
                gridcolor="#2a2a2a", linecolor="#2a2a2a",
            ),
            angularaxis=dict(
                tickfont=dict(color="#cccccc", size=11),
                gridcolor="#2a2a2a", linecolor="#333",
            ),
        ),
        paper_bgcolor="#0e0e0e",
        font_color="#cccccc",
        height=500,
        margin=dict(t=30, b=30, l=80, r=80),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Normalisé sur 100 — 100 = meilleure performance de l'équipe sur la saison")


def section(label, css_class):
    st.markdown(f'<div class="section-header {css_class}">{label}</div>', unsafe_allow_html=True)


def render_kpis_gps(row: dict, is_moyenne: bool = False):
    d = 1 if is_moyenne else 0
    section("GPS", "section-gps")
    c1, c2, c3 = st.columns(3)
    c1.metric("Distance",    fmt(row.get("distance"), suffix=" m"))
    c2.metric("Vitesse max", fmt(row.get("vitesse_max"), decimals=1, suffix=" km/h"))
    c3.metric("Sprints",     fmt(row.get("sprints"), decimals=d))
    c4, c5, c6 = st.columns(3)
    c4.metric("HSR",              fmt(row.get("hsr"), decimals=d, suffix=" m"))
    c5.metric("Charge DSL",       fmt(row.get("dsl"), decimals=1))
    c6.metric("Charge collision", fmt(row.get("collision_load"), decimals=1))


def render_kpis_tech(row: dict, is_moyenne: bool = False):
    d = 1 if is_moyenne else 0
    section("Technique", "section-tech")
    c1, c2, c3 = st.columns(3)
    c1.metric("Plaquages", pct_label(row.get("plaquages_total"), row.get("plaquages_positif"), decimals=d))
    c2.metric("Passes",    pct_label(row.get("passes_total"),    row.get("passes_positif"),    decimals=d))
    c3.metric("Porteurs",  fmt(row.get("porteur_total"),  decimals=d))
    c4, c5, c6 = st.columns(3)
    c4.metric("Soutiens",  fmt(row.get("soutiens_total"), decimals=d))
    c5.metric("Contacts",  fmt(row.get("contacts_total"), decimals=d))
    c6.metric("Essais",    fmt(row.get("essais_total"),   decimals=d))


# ── En-tête et sélecteur joueur ───────────────────────────────────────────────
df_all = load_data()

# Référence équipe pour normalisation radar (max de chaque métrique sur la saison)
_radar_cols = [c for c in RADAR_LABELS.values() if c in df_all.columns]
team_max = df_all[_radar_cols].max()

col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.png"
    if logo_path.exists():
        st.image(str(logo_path), width=90)
with col_titre:
    st.title("Rugby Data Hub — Performances")
    st.caption("Données GPS STATSports · Nationale 1")

joueurs = sorted(df_all["nom"].dropna().unique())
joueur  = st.selectbox("Joueur", joueurs)

df_joueur = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy()
df_joueur["label_match"] = (
    df_joueur["date"].dt.strftime("%d/%m/%Y") + " · " + df_joueur["adversaire"].fillna("?")
)

poste = df_joueur["poste"].dropna().iloc[0] if df_joueur["poste"].notna().any() else None
if poste:
    st.caption(f"Poste : {poste}")

st.divider()

# ── Onglets ───────────────────────────────────────────────────────────────────
tab_moy, tab_match = st.tabs(["📊  Moyenne saison", "🏉  Match"])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — MOYENNE SAISON
# ══════════════════════════════════════════════════════════════════════════════
with tab_moy:
    if df_joueur.empty:
        st.info("Aucune donnée disponible pour ce joueur.")
    else:
        stats = compute_moyenne(df_joueur)
        n = stats["n_matchs"]
        if stats["has_minutes"]:
            st.caption(f"Moyennes ramenées à 80 min · {n} match{'s' if n > 1 else ''}")
        else:
            st.caption(f"Moyennes brutes · {n} match{'s' if n > 1 else ''} (minutes jouées non disponibles)")

        render_kpis_gps(stats, is_moyenne=True)
        render_kpis_tech(stats, is_moyenne=True)

        st.divider()
        metriques_moy = st.multiselect(
            "Métriques radar",
            options=list(RADAR_LABELS.keys()),
            default=RADAR_DEFAULTS,
            key="radar_moy",
        )
        render_radar(stats, metriques_moy, team_max)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 2 — MATCH
# ══════════════════════════════════════════════════════════════════════════════
with tab_match:
    if df_joueur.empty:
        st.info("Aucune donnée disponible pour ce joueur.")
    else:
        matchs_labels = df_joueur.sort_values("date", ascending=False)["label_match"].tolist()
        match_sel = st.selectbox("Match", matchs_labels, key="match_sel")

        row = df_joueur[df_joueur["label_match"] == match_sel].iloc[0].to_dict()
        minutes = row.get("minutes_jouees")
        try:
            if not pd.isna(minutes):
                st.caption(f"{int(minutes)} min joués")
            else:
                st.caption("Minutes jouées non disponibles")
        except (TypeError, ValueError):
            st.caption("Minutes jouées non disponibles")

        render_kpis_gps(row)
        render_kpis_tech(row)

        st.divider()
        metriques_match = st.multiselect(
            "Métriques radar",
            options=list(RADAR_LABELS.keys()),
            default=RADAR_DEFAULTS,
            key="radar_match",
        )
        render_radar(row, metriques_match, team_max)
