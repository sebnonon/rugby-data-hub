"""
Dashboard Performances — Rugby Data Hub
Outil de pilotage joueur : stats brutes d'un match sélectionné et moyennes saison ramenées à 80 min.
Onglet Comparaison : évolution, agrégé saison, radar multi-métriques.
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
    page_title="Rugby Data Hub",
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
        margin: 20px 0 10px 0;
        border-radius: 4px;
    }
    .section-gps  { color: #0a7ab0; border-left: 3px solid #0a7ab0; background: #0a7ab012; }
    .section-tech { color: #E69F00; border-left: 3px solid #E69F00; background: #E69F0010; }

    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stCaption { color: #5a8aaa; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
    .block-container { padding-top: 3rem !important; }
    [data-testid="stTabs"] { margin-top: -4rem; }
    [data-testid="stRadio"] { position: relative; z-index: 10; }
    [data-testid="stSlider"] { zoom: reset; }

    /* Cartes graphiques */
    [data-testid="stPlotlyChart"] {
        border: 1.5px solid #c0d8ea;
        border-radius: 12px;
        padding: 6px 4px 4px 4px;
        background-color: #ffffff;
        box-shadow: 0 2px 8px rgba(10, 90, 140, 0.08);
        margin-bottom: 12px;
    }

</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
    font_color="#1a3a5c",
    xaxis=dict(gridcolor="#c0d8ea"), yaxis=dict(gridcolor="#c0d8ea"),
    margin=dict(t=20),
)

# ── Chargement des données — onglet Joueur ────────────────────────────────────
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
            "joueur(nom, prenom, poste_principal), match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, journee, session_title, equipe_dom)"
        )
        .execute()
    )

    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":         r["joueur_id"],
            "match_id":          r["match_id"],
            "nom":               r["joueur"]["nom"] if r["joueur"] else None,
            "prenom":            r["joueur"]["prenom"] if r["joueur"] else None,
            "poste":             r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":              r["match"]["date"] if r["match"] else None,
            "adversaire":        r["match"]["adversaire"] if r["match"] else None,
            "adversaire_complet": r["match"]["adversaire_nom_complet"] if r["match"] else None,
            "score_rec":         r["match"]["score_rec"] if r["match"] else None,
            "score_adv":         r["match"]["score_adv"] if r["match"] else None,
            "journee":           r["match"]["journee"] if r["match"] else None,
            "match_titre":       r["match"]["session_title"] if r["match"] else None,
            "equipe_dom":        r["match"]["equipe_dom"] if r["match"] else None,
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


# ── Chargement des données — onglet Comparaison ───────────────────────────────
@st.cache_data(ttl=300)
def load_match_cmp():
    c = get_client()
    cols = (
        "joueur_id, match_id, total_distance, max_speed, sprints, sprint_distance, "
        "high_speed_running_absolute, hml_distance, accelerations_absolute, decelerations_absolute, "
        "collision_load, collisions, dynamic_stress_load, metabolic_distance_absolute, "
        "distance_per_min, "
        "passes_total, passes_positif, plaquages_total, plaquages_positif, "
        "porteur_total, soutiens_total, contacts_total, grattages_total, "
        "essais_total, minutes_jouees, "
        "joueur(nom, prenom, poste_principal), match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, journee, session_title, equipe_dom)"
    )
    res = c.table("perf_match").select(cols).execute()
    rows = []
    for r in res.data:
        row = {k: r[k] for k in r if k not in ("joueur", "match")}
        row["nom"]               = r["joueur"]["nom"] if r["joueur"] else None
        row["prenom"]            = r["joueur"]["prenom"] if r["joueur"] else None
        row["poste"]             = r["joueur"]["poste_principal"] if r["joueur"] else None
        row["date"]              = r["match"]["date"] if r["match"] else None
        row["adversaire"]        = r["match"]["adversaire"] if r["match"] else None
        row["adversaire_complet"] = r["match"]["adversaire_nom_complet"] if r["match"] else None
        row["score_rec"]         = r["match"]["score_rec"] if r["match"] else None
        row["score_adv"]         = r["match"]["score_adv"] if r["match"] else None
        row["journee"]           = r["match"]["journee"] if r["match"] else None
        row["match_titre"]       = r["match"]["session_title"] if r["match"] else None
        row["equipe_dom"]        = r["match"]["equipe_dom"] if r["match"] else None
        rows.append(row)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.rename(columns={
        "total_distance":              "distance",
        "max_speed":                   "vitesse_max",
        "high_speed_running_absolute": "hsr",
        "dynamic_stress_load":         "dsl",
    })
    return df.sort_values("date")


@st.cache_data(ttl=300)
def load_entr_cmp():
    c = get_client()
    cols = (
        "joueur_id, date, session_type, seance_type, "
        "total_distance, max_speed, sprints, sprint_distance, "
        "high_speed_running_absolute, hml_distance, accelerations_absolute, decelerations_absolute, "
        "dynamic_stress_load, metabolic_distance_absolute, distance_per_min, "
        "joueur(nom, prenom, poste_principal)"
    )
    res = c.table("perf_entrainement").select(cols).execute()
    rows = []
    for r in res.data:
        row = {k: r[k] for k in r if k not in ("joueur",)}
        row["nom"]    = r["joueur"]["nom"] if r["joueur"] else None
        row["prenom"] = r["joueur"]["prenom"] if r["joueur"] else None
        row["poste"]  = r["joueur"]["poste_principal"] if r["joueur"] else None
        rows.append(row)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


# ── Métriques disponibles pour la comparaison ─────────────────────────────────
METRIQUES_MATCH_CMP = {
    "Distance totale (m)":       "total_distance",
    "Vitesse max (km/h)":        "max_speed",
    "Sprints":                   "sprints",
    "Distance sprint (m)":       "sprint_distance",
    "HSR (m)":                   "high_speed_running_absolute",
    "Distance HML (m)":          "hml_distance",
    "Accélérations":             "accelerations_absolute",
    "Décélérations":             "decelerations_absolute",
    "Charge de collision":       "collision_load",
    "DSL":                       "dynamic_stress_load",
    "Distance métabolique (m)":  "metabolic_distance_absolute",
    "Passes (total)":            "passes_total",
    "Passes (positives)":        "passes_positif",
    "Plaquages (total)":         "plaquages_total",
    "Plaquages (positifs)":      "plaquages_positif",
    "Porteur (total)":           "porteur_total",
    "Soutiens (total)":          "soutiens_total",
    "Contacts (total)":          "contacts_total",
    "Essais":                    "essais_total",
    "Minutes jouées":            "minutes_jouees",
}

METRIQUES_ENTR_CMP = {
    "Distance totale (m)":       "total_distance",
    "Vitesse max (km/h)":        "max_speed",
    "Sprints":                   "sprints",
    "Distance sprint (m)":       "sprint_distance",
    "HSR (m)":                   "high_speed_running_absolute",
    "Distance HML (m)":          "hml_distance",
    "Accélérations":             "accelerations_absolute",
    "Décélérations":             "decelerations_absolute",
    "DSL":                       "dynamic_stress_load",
    "Distance métabolique (m)":  "metabolic_distance_absolute",
    "Vitesse moy. (m/min)":      "distance_per_min",
}

SOURCES_CMP = {
    "Matchs (GPS + actions)": ("match", METRIQUES_MATCH_CMP),
    "Entraînements":          ("entr",  METRIQUES_ENTR_CMP),
}

def _get_saison(d):
    return f"{d.year}/{d.year+1}" if d.month >= 8 else f"{d.year-1}/{d.year}"


RADAR_COLORS = [
    "#56B4E9", "#E69F00", "#009E73", "#CC79A7",
    "#F0E442", "#D55E00", "#0072B2", "#7FDBFF",
]


# ── Helpers — onglet Joueur ───────────────────────────────────────────────────

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


def pct_label(total, positif, decimals=0):
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

RADAR_LABELS: dict[str, str] = {**GPS_LABELS, **TECH_LABELS}

GPS_DEFAULTS:  list[str] = list(GPS_LABELS.keys())
TECH_DEFAULTS: list[str] = ["Plaquages", "Passes", "Porteurs", "Soutiens", "Contacts", "Essais"]

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

_PER_80_COLS = [
    "distance", "sprints", "hsr", "dsl", "collision_load",
    "passes_total", "passes_positif",
    "plaquages_total", "plaquages_positif",
    "porteur_total", "soutiens_total", "contacts_total", "essais_total",
]


def compute_moyenne(df_joueur: pd.DataFrame) -> dict:
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


def compute_poste_median_moy(df_poste: pd.DataFrame, cols: list) -> pd.Series:
    """Médiane des moyennes par joueur (pour le mode Moyenne)."""
    avg_rows = []
    for p in df_poste["nom"].dropna().unique():
        avg = compute_moyenne(df_poste[df_poste["nom"] == p])
        avg_rows.append({c: avg.get(c) for c in cols})
    if not avg_rows:
        return pd.Series(dtype=float)
    return pd.DataFrame(avg_rows).median()


def render_radar(vals: dict, selected_labels: list, team_ref: pd.Series, team_med: pd.Series | None = None, med_label: str = "Médiane poste"):
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
                vals_norm.append(min(round(float(raw) / ref * 100, 1), 100))
                vals_txt.append(f"{float(raw):.1f}")
        except (TypeError, ValueError):
            vals_norm.append(0)
            vals_txt.append("—")

    theta = selected_labels + [selected_labels[0]]
    r_plot = vals_norm + [vals_norm[0]]
    t_plot = vals_txt  + [vals_txt[0]]

    fig = go.Figure()

    # Contour médiane équipe
    if team_med is not None:
        med_norm, med_txt = [], []
        for col in cols_r:
            ref = float(team_ref.get(col, 0) or 0)
            m = float(team_med.get(col, 0) or 0)
            if ref > 0:
                med_norm.append(min(round(m / ref * 100, 1), 110))
                med_txt.append(f"{m:.1f}")
            else:
                med_norm.append(0)
                med_txt.append("—")
        fig.add_trace(go.Scatterpolar(
            r=med_norm + [med_norm[0]], theta=theta,
            fill="none",
            line=dict(color="#aaaaaa", width=1.5, dash="dot"),
            text=med_txt + [med_txt[0]],
            name=med_label,
            hovertemplate="%{theta} : %{text}<extra>" + med_label + "</extra>",
        ))

    fig.add_trace(go.Scatterpolar(
        r=r_plot, theta=theta,
        fill="toself",
        fillcolor="rgba(10,122,176,0.10)",
        line=dict(color="#0a7ab0", width=2.5),
        text=t_plot,
        name="Joueur",
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
        height=580,
        margin=dict(t=30, b=50, l=80, r=80),
        showlegend=team_med is not None,
        legend=dict(
            orientation="h", y=-0.1,
            font=dict(color="#1a3a5c", size=11),
            title=dict(text="Normalisé sur 100 (100 = max équipe) —", font=dict(size=10, color="#5a8aaa")),
        ),
    )
    st.plotly_chart(fig, use_container_width=True)


def section(label, css_class):
    st.markdown(f'<div class="section-header {css_class}">{label}</div>', unsafe_allow_html=True)


def render_kpis_section(
    row: dict,
    selected_labels: list[str],
    section_label: str,
    css_class: str,
    is_moyenne: bool = False,
    all_labels: dict[str, str] = RADAR_LABELS,
    ncols: int = 3,
):
    section(section_label, css_class)
    if not selected_labels:
        st.caption("Aucune métrique sélectionnée.")
        return
    for i in range(0, len(selected_labels), ncols):
        chunk = selected_labels[i:i+ncols]
        cols = st.columns(ncols)
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


# ── En-tête ───────────────────────────────────────────────────────────────────
df_all = load_data()

_radar_cols = [c for c in RADAR_LABELS.values() if c in df_all.columns]
team_max = df_all[_radar_cols].max()

df_joueurs_ref = (
    df_all[["nom", "prenom"]].drop_duplicates()
    .dropna(subset=["nom"])
    .sort_values("nom")
)
df_joueurs_ref["label"] = df_joueurs_ref.apply(
    lambda r: f"{r['prenom']} {r['nom']}" if pd.notna(r["prenom"]) else r["nom"], axis=1
)
joueur_labels = df_joueurs_ref["label"].tolist()
label_to_nom  = dict(zip(df_joueurs_ref["label"], df_joueurs_ref["nom"]))


_c_titre, _c_vue = st.columns([7, 2])
with _c_titre:
    st.markdown(
        '<p style="font-weight:700; font-size:1.1rem; color:#071626; margin:0; padding:0.5rem 0 0 0; text-align:center;">Performances joueur</p>',
        unsafe_allow_html=True,
    )
with _c_vue:
    st.markdown('<div style="height:0.35rem"></div>', unsafe_allow_html=True)
    vue = st.radio(
        "Vue", ["🏉 Match", "📊 Moyenne"],
        horizontal=True, label_visibility="collapsed", key="j_vue",
    )

tab_joueur, tab_compare = st.tabs(["📊 Joueur", "⚖️ Comparaison"])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET JOUEUR
# ══════════════════════════════════════════════════════════════════════════════
with tab_joueur:
    # ── Ligne du haut : sélecteurs + encadrés ─────────────────────────────────
    if vue == "🏉 Match":
        c1, c2, c_j, c_s, c_t, c_p = st.columns([1.8, 2.5, 1.3, 1.6, 1.5, 1.3], gap="small")
    else:
        c1, c2, c_date, c_p = st.columns([1.8, 2.5, 4.4, 1.3], gap="small")

    with c1:
        joueur_label = st.selectbox("Joueur", joueur_labels, key="j_joueur")

    joueur = label_to_nom[joueur_label]
    df_joueur = df_all[df_all["nom"] == joueur].dropna(subset=["date"]).copy()

    def _make_match_label(row):
        date_str = row["date"].strftime("%d/%m/%y")
        nom_adv = row.get("adversaire_complet") or row.get("adversaire") or "?"
        return f"{nom_adv} · {date_str}"

    df_joueur["label_match"] = df_joueur.apply(_make_match_label, axis=1)
    matchs_labels = df_joueur.sort_values("date", ascending=False)["label_match"].tolist()

    _poste_raw = df_joueur["poste"].dropna().iloc[0] if not df_joueur.empty and df_joueur["poste"].notna().any() else None
    _df_poste = df_all[df_all["poste"] == _poste_raw] if _poste_raw else df_all
    if vue == "🏉 Match":
        poste_median = None  # calculé après sélection du match
    else:
        poste_median = compute_poste_median_moy(_df_poste, _radar_cols)
    _poste = str(_poste_raw) if _poste_raw else "—"

    if vue == "🏉 Match":
        with c2:
            match_sel = st.selectbox(
                "Match", matchs_labels if matchs_labels else ["—"],
                key="j_match_sel", disabled=not matchs_labels,
            )

        if not df_joueur.empty and matchs_labels:
            _row_info = df_joueur[df_joueur["label_match"] == match_sel].iloc[0]
            _journee = _row_info.get("journee")
            try:
                _journee_str = f"J{int(_journee)}" if pd.notna(_journee) else "—"
            except (TypeError, ValueError):
                _journee_str = "—"
            sr, sa = _row_info.get("score_rec"), _row_info.get("score_adv")
            try:
                _score = f"{int(sr)} — {int(sa)}" if (pd.notna(sr) and pd.notna(sa)) else "—"
            except (TypeError, ValueError):
                _score = "—"
            _dom = _row_info.get("equipe_dom") or ""
            _lieu_icon = "🏠" if "REC" in str(_dom).upper() else "✈️"
            _minutes = _row_info.get("minutes_jouees")
            try:
                _min_str = f"{int(_minutes)} min" if pd.notna(_minutes) else "—"
            except (TypeError, ValueError):
                _min_str = "—"
            _poste = _row_info.get("poste") or "—"
        else:
            _journee_str = _score = _min_str = "—"

        c_j.metric("Journée", _journee_str)
        c_s.metric(f"Score {_lieu_icon}", _score)
        c_t.metric("Temps de jeu", _min_str)
        c_p.metric("Poste", _poste)

    else:  # Vue Moyenne

        if not df_joueur.empty:
            df_joueur["saison"] = df_joueur["date"].apply(_get_saison)
            saisons_dispo = sorted(df_joueur["saison"].unique(), reverse=True)
        else:
            saisons_dispo = []

        with c2:
            saison_sel = st.selectbox("Saison", ["Toutes"] + saisons_dispo, key="j_saison")

        df_filtre = df_joueur if saison_sel == "Toutes" else df_joueur[df_joueur["saison"] == saison_sel]
        d_min = df_filtre["date"].min().date() if not df_filtre.empty else pd.Timestamp.today().date()
        d_max = df_filtre["date"].max().date() if not df_filtre.empty else pd.Timestamp.today().date()

        with c_date:
            dates_dispo = sorted(df_filtre["date"].dt.date.unique()) if not df_filtre.empty else [d_min]
            date_range = st.select_slider(
                "Période",
                options=dates_dispo,
                value=(dates_dispo[0], dates_dispo[-1]),
                format_func=lambda d: d.strftime("%d/%m/%y"),
                key=f"j_sel2_{joueur}_{saison_sel}",
            )

        if len(date_range) == 2:
            df_joueur = df_joueur[
                (df_joueur["date"].dt.date >= date_range[0]) &
                (df_joueur["date"].dt.date <= date_range[1])
            ]

        c_p.metric("Poste", _poste)

    # ── Section centrale : Radar (gauche) + KPIs (droite) ─────────────────────
    if vue == "🏉 Match":
        if df_joueur.empty or not matchs_labels:
            st.info("Aucune donnée disponible pour ce joueur.")
        else:
            _row_sel = df_joueur[df_joueur["label_match"] == match_sel].iloc[0]
            row = _row_sel.to_dict()
            _mid_j = _row_sel["match_id"]
            poste_median = df_all[df_all["match_id"] == _mid_j][_radar_cols].median()

            gps_sel = st.session_state.get("j_gps_match", GPS_DEFAULTS)
            tech_sel = st.session_state.get("j_tech_match", TECH_DEFAULTS)

            col_radar, col_kpis = st.columns([7, 4], gap="medium")
            with col_radar:
                render_radar(row, gps_sel + tech_sel, team_max, poste_median, med_label="Médiane match")
            with col_kpis:
                render_kpis_section(row, gps_sel,  "GPS",       "section-gps",  all_labels=GPS_LABELS,  ncols=3)
                render_kpis_section(row, tech_sel, "Technique", "section-tech", all_labels=TECH_LABELS, ncols=3)

            st.divider()
            cg, ct = st.columns(2)
            with cg:
                st.multiselect(
                    "Métriques GPS", list(GPS_LABELS.keys()),
                    default=GPS_DEFAULTS, key="j_gps_match",
                )
            with ct:
                st.multiselect(
                    "Métriques Technique", list(TECH_LABELS.keys()),
                    default=TECH_DEFAULTS, key="j_tech_match",
                )

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

            gps_sel = st.session_state.get("j_gps_moy", GPS_DEFAULTS)
            tech_sel = st.session_state.get("j_tech_moy", TECH_DEFAULTS)

            col_radar, col_kpis = st.columns([7, 4], gap="medium")
            with col_radar:
                render_radar(stats, gps_sel + tech_sel, team_max, poste_median, med_label="Médiane poste")
            with col_kpis:
                render_kpis_section(stats, gps_sel,  "GPS",       "section-gps",  is_moyenne=True, all_labels=GPS_LABELS,  ncols=3)
                render_kpis_section(stats, tech_sel, "Technique", "section-tech", is_moyenne=True, all_labels=TECH_LABELS, ncols=3)

            st.divider()
            cg, ct = st.columns(2)
            with cg:
                st.multiselect(
                    "Métriques GPS", list(GPS_LABELS.keys()),
                    default=GPS_DEFAULTS, key="j_gps_moy",
                )
            with ct:
                st.multiselect(
                    "Métriques Technique", list(TECH_LABELS.keys()),
                    default=TECH_DEFAULTS, key="j_tech_moy",
                )


# ══════════════════════════════════════════════════════════════════════════════
# ONGLET COMPARAISON
# ══════════════════════════════════════════════════════════════════════════════
with tab_compare:
    df_src_c = load_match_cmp()

    def _joueur_label_cmp(row):
        return f"{row['prenom']} {row['nom']}" if pd.notna(row.get("prenom")) else row["nom"]

    avail_gps  = [k for k in GPS_LABELS  if GPS_LABELS[k]  in df_src_c.columns]
    avail_tech = [k for k in TECH_LABELS if TECH_LABELS[k] in df_src_c.columns]

    # ── Ligne du haut selon le mode ───────────────────────────────────────────
    if vue == "🏉 Match":
        def _match_label_c(row):
            adv = row.get("adversaire_complet") or row.get("adversaire") or "?"
            return f"{adv} · {row['date'].strftime('%d/%m/%y')}"

        matchs_ref_c = (
            df_src_c[["match_id", "date", "adversaire", "adversaire_complet", "journee", "score_rec", "score_adv", "equipe_dom"]]
            .drop_duplicates("match_id").dropna(subset=["date"]).sort_values("date", ascending=False)
        )
        matchs_ref_c["label"] = matchs_ref_c.apply(_match_label_c, axis=1)
        matchs_labels_c = matchs_ref_c["label"].tolist()
        label_to_mid_c  = dict(zip(matchs_ref_c["label"], matchs_ref_c["match_id"]))

        c_mc, c_jc, c_sc = st.columns([6, 1.5, 1.5], gap="small")
        with c_mc:
            match_sel_c = st.selectbox("Match", matchs_labels_c if matchs_labels_c else ["—"], key="cp_match")

        mid_c = label_to_mid_c.get(match_sel_c)
        if mid_c:
            _mref = matchs_ref_c[matchs_ref_c["match_id"] == mid_c].iloc[0]
            _j = _mref.get("journee")
            try:
                _journee_c = f"J{int(_j)}" if pd.notna(_j) else "—"
            except (TypeError, ValueError):
                _journee_c = "—"
            sr, sa = _mref.get("score_rec"), _mref.get("score_adv")
            try:
                _score_c = f"{int(sr)} — {int(sa)}" if (pd.notna(sr) and pd.notna(sa)) else "—"
            except (TypeError, ValueError):
                _score_c = "—"
            _dom_c = _mref.get("equipe_dom") or ""
            _lieu_icon_c = "🏠" if "REC" in str(_dom_c).upper() else "✈️"
        else:
            _journee_c = _score_c = "—"
            _lieu_icon_c = ""
        c_jc.metric("Journée", _journee_c)
        c_sc.metric(f"Score {_lieu_icon_c}".strip(), _score_c)

        df_cmp = df_src_c[df_src_c["match_id"] == mid_c] if mid_c else df_src_c
        key_joueurs = f"cp_joueurs_{mid_c}"

    else:  # Moyenne
        df_src_c["saison"] = df_src_c["date"].apply(_get_saison)
        saisons_all_c = sorted(df_src_c["saison"].dropna().unique(), reverse=True)

        c_sais_c, c_date_c = st.columns([2, 7], gap="small")
        with c_sais_c:
            saison_sel_c = st.selectbox("Saison", ["Toutes"] + saisons_all_c, key="cp_saison")

        df_fil_c = df_src_c if saison_sel_c == "Toutes" else df_src_c[df_src_c["saison"] == saison_sel_c]
        dates_dispo_c = sorted(df_fil_c["date"].dt.date.unique()) if not df_fil_c.empty else [pd.Timestamp.today().date()]

        with c_date_c:
            date_range_c = st.select_slider(
                "Période", options=dates_dispo_c,
                value=(dates_dispo_c[0], dates_dispo_c[-1]),
                format_func=lambda d: d.strftime("%d/%m/%y"),
                key=f"cp_date_{saison_sel_c}",
            )

        if len(date_range_c) == 2:
            df_cmp = df_fil_c[
                (df_fil_c["date"].dt.date >= date_range_c[0]) &
                (df_fil_c["date"].dt.date <= date_range_c[1])
            ]
        else:
            df_cmp = df_fil_c
        key_joueurs = f"cp_joueurs_moy_{saison_sel_c}"

    # ── Sélection joueurs ──────────────────────────────────────────────────────
    df_j_ref_c = df_cmp[["nom", "prenom"]].drop_duplicates().dropna(subset=["nom"]).sort_values("nom")
    df_j_ref_c["label"] = df_j_ref_c.apply(_joueur_label_cmp, axis=1)
    joueurs_dispo_c = df_j_ref_c["label"].tolist()
    label_to_nom_c  = dict(zip(df_j_ref_c["label"], df_j_ref_c["nom"]))

    joueurs_labels_sel = st.multiselect(
        "Joueurs à comparer", joueurs_dispo_c,
        default=joueurs_dispo_c[:3] if len(joueurs_dispo_c) >= 3 else joueurs_dispo_c,
        max_selections=8, key=key_joueurs,
    )
    joueurs_sel_c = [label_to_nom_c[l] for l in joueurs_labels_sel]

    # ── Métriques (lues depuis session_state, widgets placés en bas) ───────────
    gps_sel_c  = st.session_state.get("cp_gps",  avail_gps)
    tech_sel_c = st.session_state.get("cp_tech", TECH_DEFAULTS if avail_tech else [])

    all_labels_c  = {**GPS_LABELS, **TECH_LABELS}
    metriques_sel = [m for m in (gps_sel_c + tech_sel_c) if all_labels_c.get(m) in df_src_c.columns]
    cols_r        = [all_labels_c[m] for m in metriques_sel]

    if not joueurs_sel_c:
        st.info("Sélectionne au moins un joueur.")
    elif len(metriques_sel) < 3:
        st.info("Sélectionne au moins 3 métriques.")
    else:
        df_all_c = load_match_cmp()
        max_eq   = {col: df_all_c[col].max() for col in cols_r if col in df_all_c.columns}

        # Données par joueur selon le mode
        if vue == "🏉 Match":
            df_r   = df_cmp[df_cmp["nom"].isin(joueurs_sel_c)].copy()
            df_idx = df_r.set_index("nom")
            postes_c = df_r["poste"].dropna().unique().tolist() if "poste" in df_r.columns else []
            def _get_vals(joueur_nom):
                if joueur_nom not in df_idx.index:
                    return None, None
                row_j = df_idx.loc[joueur_nom]
                if isinstance(row_j, pd.DataFrame):
                    row_j = row_j.iloc[0]
                return row_j.to_dict(), False
        else:
            stats_map = {}
            postes_c  = []
            for jn in joueurs_sel_c:
                df_j = df_cmp[df_cmp["nom"] == jn]
                stats_map[jn] = compute_moyenne(df_j)
                p = df_j["poste"].dropna().iloc[0] if not df_j.empty and df_j["poste"].notna().any() else None
                if p and p not in postes_c:
                    postes_c.append(p)
            def _get_vals(joueur_nom):
                return stats_map.get(joueur_nom, {}), True

        _cols_ok     = all(c in df_all_c.columns for c in cols_r)
        _med_label_c = "Médiane équipe"
        if vue == "🏉 Match":
            _df_mid_c = df_all_c[df_all_c["match_id"] == mid_c] if mid_c else df_all_c
            med_c     = _df_mid_c[cols_r].median() if _cols_ok else pd.Series(dtype=float)
        else:
            med_c = compute_poste_median_moy(df_all_c, cols_r) if _cols_ok else pd.Series(dtype=float)

        fig = go.Figure()
        theta = metriques_sel + [metriques_sel[0]]

        if not med_c.empty:
            vals_med = [min(float(med_c.get(c, 0) or 0) / max_eq.get(c, 1) * 100, 100) for c in cols_r]
            fig.add_trace(go.Scatterpolar(
                r=vals_med + [vals_med[0]], theta=theta,
                name=_med_label_c,
                text=[f"{float(med_c.get(c,0) or 0):.1f}" for c in cols_r] + [f"{float(med_c.get(cols_r[0],0) or 0):.1f}"],
                hovertemplate="%{theta} : %{text}<extra>" + _med_label_c + "</extra>",
                line=dict(color="#aaaaaa", width=1.5, dash="dot"), fill="none",
            ))

        for i, joueur_nom in enumerate(joueurs_sel_c):
            row_dict, _ = _get_vals(joueur_nom)
            if not row_dict:
                continue
            vals_raw = [float(row_dict.get(c, 0) or 0) for c in cols_r]
            vals     = [min(v / max_eq.get(c, 1) * 100, 100) if max_eq.get(c, 1) > 0 else 0 for v, c in zip(vals_raw, cols_r)]
            color    = RADAR_COLORS[i % len(RADAR_COLORS)]
            r, g, b  = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
            fig.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=theta,
                name=joueurs_labels_sel[i],
                text=[f"{v:.1f}" for v in vals_raw] + [f"{vals_raw[0]:.1f}"],
                hovertemplate="%{theta} : %{text}<extra>%{fullData.name}</extra>",
                line=dict(color=color, width=2),
                fill="toself", fillcolor=f"rgba({r},{g},{b},0.15)",
            ))

        fig.update_layout(
            polar=dict(
                bgcolor="#ffffff",
                radialaxis=dict(visible=True, range=[0, 100], tickvals=[25, 50, 75, 100],
                                tickfont=dict(color="#8ab4c8", size=9), gridcolor="#c0d8ea", linecolor="#c0d8ea"),
                angularaxis=dict(tickfont=dict(color="#1a3a5c", size=11), gridcolor="#c0d8ea", linecolor="#333"),
            ),
            paper_bgcolor="#f0f6fb", font_color="#1a3a5c",
            legend=dict(orientation="h", y=-0.1, font=dict(color="#1a3a5c", size=11),
                        title=dict(text="Normalisé sur 100 (100 = max équipe) —", font=dict(size=10, color="#5a8aaa"))),
            height=580, margin=dict(t=30, b=80, l=80, r=80),
        )
        st.plotly_chart(fig, use_container_width=True)

        for i, joueur_nom in enumerate(joueurs_sel_c):
            row_dict, is_moy = _get_vals(joueur_nom)
            if not row_dict:
                continue
            color = RADAR_COLORS[i % len(RADAR_COLORS)]
            st.markdown(
                f'<p style="font-weight:700; font-size:0.85rem; color:{color}; margin:8px 0 2px 0;">'
                f'{joueurs_labels_sel[i]}</p>',
                unsafe_allow_html=True,
            )
            _all_sel = [m for m in (gps_sel_c + tech_sel_c) if all_labels_c.get(m)]
            _ncols   = max((len(_all_sel) + 1) // 2, 1)
            for _i in range(0, len(_all_sel), _ncols):
                _chunk = _all_sel[_i:_i + _ncols]
                _cols  = st.columns(len(_chunk))
                for _j, _lbl in enumerate(_chunk):
                    _col  = all_labels_c.get(_lbl)
                    _meta = METRIC_META.get(_col, {"label": _lbl, "suffix": "", "decimals": 0})
                    _d    = 1 if is_moy else _meta.get("decimals", 0)
                    if "pct_col" in _meta:
                        _val = pct_label(row_dict.get(_col), row_dict.get(_meta["pct_col"]), decimals=_d)
                    else:
                        _val = fmt(row_dict.get(_col), decimals=_d, suffix=_meta.get("suffix", ""))
                    _cols[_j].metric(_meta["label"], _val)

        st.divider()
        cg_c, ct_c = st.columns(2)
        with cg_c:
            st.multiselect("Métriques GPS", avail_gps, default=avail_gps, key="cp_gps")
        with ct_c:
            st.multiselect("Métriques Technique", avail_tech, default=TECH_DEFAULTS if avail_tech else [], key="cp_tech")
