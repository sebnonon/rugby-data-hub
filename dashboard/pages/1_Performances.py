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
    page_title="Performances joueur",
    page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg") if (Path(__file__).parent.parent / "logo.jpg").exists() else "🏉",
    layout="wide",
)

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; zoom: 0.9; }
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
    .block-container { padding-top: 3rem !important; }
    [data-testid="stTabs"] { margin-top: -4rem; }

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
            "joueur(nom, prenom, poste_principal), match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, journee, session_title)"
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
        "joueur(nom, prenom, poste_principal), match(date, adversaire, session_title)"
    )
    res = c.table("perf_match").select(cols).execute()
    rows = []
    for r in res.data:
        row = {k: r[k] for k in r if k not in ("joueur", "match")}
        row["nom"]         = r["joueur"]["nom"] if r["joueur"] else None
        row["prenom"]      = r["joueur"]["prenom"] if r["joueur"] else None
        row["poste"]       = r["joueur"]["poste_principal"] if r["joueur"] else None
        row["date"]        = r["match"]["date"] if r["match"] else None
        row["adversaire"]  = r["match"]["adversaire"] if r["match"] else None
        row["match_titre"] = r["match"]["session_title"] if r["match"] else None
        rows.append(row)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
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
        height=380,
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
    c1, c2, c_j, c_s, c_t, c_p = st.columns([2.5, 3.5, 1.1, 1.5, 1.3, 1.3], gap="small")

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

    with c2:
        match_sel = st.selectbox(
            "Match", matchs_labels if matchs_labels else ["—"],
            key="j_match_sel", disabled=(not matchs_labels or vue == "📊 Moyenne"),
        )

    # Encadrés — toujours visibles, « — » en vue Moyenne sauf Poste
    if not df_joueur.empty and matchs_labels and vue == "🏉 Match":
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
        _minutes = _row_info.get("minutes_jouees")
        try:
            _min_str = f"{int(_minutes)} min" if pd.notna(_minutes) else "—"
        except (TypeError, ValueError):
            _min_str = "—"
        _poste = _row_info.get("poste") or "—"
    else:
        _journee_str = "—"
        _score = "—"
        _min_str = "—"
        _poste_raw = df_joueur["poste"].dropna().iloc[0] if not df_joueur.empty and df_joueur["poste"].notna().any() else None
        _poste = str(_poste_raw) if _poste_raw else "—"

    c_j.metric("Journée", _journee_str)
    c_s.metric("Score", _score)
    c_t.metric("Temps de jeu", _min_str)
    c_p.metric("Poste", _poste)

    # ── Section centrale : Radar (gauche) + KPIs (droite) ─────────────────────
    if vue == "🏉 Match":
        if df_joueur.empty or not matchs_labels:
            st.info("Aucune donnée disponible pour ce joueur.")
        else:
            row = df_joueur[df_joueur["label_match"] == match_sel].iloc[0].to_dict()

            gps_sel = st.session_state.get("j_gps_match", GPS_DEFAULTS)
            tech_sel = st.session_state.get("j_tech_match", TECH_DEFAULTS)

            col_radar, col_kpis = st.columns([5, 4], gap="medium")
            with col_radar:
                render_radar(row, gps_sel + tech_sel, team_max)
            with col_kpis:
                render_kpis_section(row, gps_sel,  "GPS",       "section-gps",  all_labels=GPS_LABELS,  ncols=2)
                render_kpis_section(row, tech_sel, "Technique", "section-tech", all_labels=TECH_LABELS, ncols=2)

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

            col_radar, col_kpis = st.columns([5, 4], gap="medium")
            with col_radar:
                render_radar(stats, gps_sel + tech_sel, team_max)
            with col_kpis:
                render_kpis_section(stats, gps_sel,  "GPS",       "section-gps",  is_moyenne=True, all_labels=GPS_LABELS,  ncols=2)
                render_kpis_section(stats, tech_sel, "Technique", "section-tech", is_moyenne=True, all_labels=TECH_LABELS, ncols=2)

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
    c1, c2 = st.columns([2, 2])
    with c1:
        source_label_c = st.selectbox("Source", list(SOURCES_CMP.keys()), key="cp_source")
    source_key_c, metriques_c = SOURCES_CMP[source_label_c]
    with c2:
        metrique_label_c = st.selectbox("Métrique", list(metriques_c.keys()), key="cp_metrique")
    metrique_col_c = metriques_c[metrique_label_c]

    df_src_c = load_match_cmp() if source_key_c == "match" else load_entr_cmp()

    # Slider période
    if "date" in df_src_c.columns and not df_src_c["date"].isna().all():
        d_min_c = df_src_c["date"].min().date()
        d_max_c = df_src_c["date"].max().date()
        d_from_c, d_to_c = st.slider(
            "Période",
            min_value=d_min_c, max_value=d_max_c,
            value=(d_min_c, d_max_c),
            format="DD/MM/YY",
            key="cp_periode",
        )
        df_src_c = df_src_c[
            (df_src_c["date"].dt.date >= d_from_c) & (df_src_c["date"].dt.date <= d_to_c)
        ]

    # Labels "Prénom NOM" pour le multiselect
    def _joueur_label_cmp(row):
        return f"{row['prenom']} {row['nom']}" if pd.notna(row.get("prenom")) else row["nom"]

    if "prenom" in df_src_c.columns:
        df_j_ref_c = df_src_c[["nom", "prenom"]].drop_duplicates().dropna(subset=["nom"]).sort_values("nom")
        df_j_ref_c["label"] = df_j_ref_c.apply(_joueur_label_cmp, axis=1)
        joueurs_dispo_c = df_j_ref_c["label"].tolist()
        label_to_nom_c  = dict(zip(df_j_ref_c["label"], df_j_ref_c["nom"]))
    else:
        joueurs_dispo_c = sorted(df_src_c["nom"].dropna().unique())
        label_to_nom_c  = {j: j for j in joueurs_dispo_c}

    joueurs_labels_sel = st.multiselect(
        "Joueurs à comparer", joueurs_dispo_c,
        default=joueurs_dispo_c[:3] if len(joueurs_dispo_c) >= 3 else joueurs_dispo_c,
        max_selections=8, key="cp_joueurs",
    )
    joueurs_sel_c = [label_to_nom_c[l] for l in joueurs_labels_sel]

    vue_c = st.radio(
        "Vue", ["Évolution dans le temps", "Agrégé saison", "Radar (multi-métriques)"],
        horizontal=True, key="cp_vue",
    )

    if not joueurs_sel_c:
        st.info("Sélectionne au moins un joueur.")

    elif vue_c == "Radar (multi-métriques)":
        metriques_dispo_radar = [
            m for m, col in metriques_c.items()
            if col in df_src_c.columns and col != "_count"
        ]
        metriques_radar_sel = st.multiselect(
            "Métriques à afficher sur le radar",
            metriques_dispo_radar,
            default=metriques_dispo_radar[:6],
            key="cp_radar_metriques",
        )
        ref_mediane = st.radio(
            "Référence médiane",
            ["Aucune", "Médiane équipe", "Médiane poste"],
            horizontal=True, key="cp_radar_ref",
        )

        if len(metriques_radar_sel) < 3:
            st.info("Sélectionne au moins 3 métriques.")
        else:
            cols_r   = [metriques_c[m] for m in metriques_radar_sel]
            df_r     = df_src_c[df_src_c["nom"].isin(joueurs_sel_c)].copy()
            df_agg_r = df_r.groupby("nom")[cols_r].mean()

            df_equipe_agg = df_src_c.groupby("nom")[cols_r].mean()
            max_equipe = {col: df_equipe_agg[col].max() for col in cols_r}

            df_norm = df_agg_r.copy()
            for col in cols_r:
                mx = max_equipe[col]
                df_norm[col] = (df_agg_r[col] / mx * 100) if mx > 0 else 0

            df_mediane = None
            label_mediane = ""
            if ref_mediane == "Médiane équipe":
                df_mediane = df_equipe_agg.median()
                label_mediane = "Médiane équipe"
            elif ref_mediane == "Médiane poste" and "poste" in df_src_c.columns:
                postes_sel = df_src_c[df_src_c["nom"].isin(joueurs_sel_c)]["poste"].dropna().unique()
                df_poste = df_src_c[df_src_c["poste"].isin(postes_sel)]
                df_mediane = df_poste.groupby("nom")[cols_r].mean().median()
                label_mediane = f"Médiane {' / '.join(postes_sel)}"

            fig = go.Figure()
            theta = metriques_radar_sel + [metriques_radar_sel[0]]

            if df_mediane is not None:
                vals_med = [df_mediane[c] / max_equipe[c] * 100 if max_equipe[c] > 0 else 0 for c in cols_r]
                vals_med_raw = [f"{df_mediane[c]:.1f}" for c in cols_r]
                fig.add_trace(go.Scatterpolar(
                    r=vals_med + [vals_med[0]],
                    theta=theta,
                    name=label_mediane,
                    text=vals_med_raw + [vals_med_raw[0]],
                    hovertemplate="%{theta}<br>%{text}<extra>" + label_mediane + "</extra>",
                    line=dict(color="#0a7ab0", width=2, dash="dash"),
                    fill="none",
                ))

            for i, joueur_nom in enumerate(joueurs_sel_c):
                if joueur_nom not in df_norm.index:
                    continue
                vals = [df_norm.loc[joueur_nom, c] for c in cols_r]
                vals_raw = [df_agg_r.loc[joueur_nom, c] for c in cols_r]
                vals_txt = [f"{v:.1f}" for v in vals_raw]
                color = RADAR_COLORS[i % len(RADAR_COLORS)]
                label_j = joueurs_labels_sel[joueurs_sel_c.index(joueur_nom)]
                fig.add_trace(go.Scatterpolar(
                    r=vals + [vals[0]],
                    theta=theta,
                    name=label_j,
                    text=vals_txt + [vals_txt[0]],
                    hovertemplate="%{theta}<br>%{text}<extra>%{fullData.name}</extra>",
                    line=dict(color=color, width=2),
                    fill="toself",
                    fillcolor=color,
                    opacity=0.15,
                ))

            fig.update_layout(
                polar=dict(
                    bgcolor="#ffffff",
                    radialaxis=dict(
                        visible=True, range=[0, 100],
                        tickfont=dict(color="#666666", size=9),
                        gridcolor="#c0d8ea",
                    ),
                    angularaxis=dict(
                        tickfont=dict(color="#1a3a5c", size=11),
                        gridcolor="#c0d8ea",
                        linecolor="#c0d8ea",
                    ),
                ),
                paper_bgcolor="#f0f6fb",
                font_color="#1a3a5c",
                legend=dict(orientation="h", y=-0.1, font_color="#1a3a5c"),
                height=520,
                margin=dict(t=40, b=60, l=60, r=60),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Valeurs normalisées sur 100 (100 = meilleur de l'équipe pour cette métrique)")

    elif metrique_col_c not in df_src_c.columns:
        st.warning(f"Métrique `{metrique_col_c}` absente des données.")

    else:
        df_c = df_src_c[df_src_c["nom"].isin(joueurs_sel_c)].dropna(subset=[metrique_col_c, "date"])

        if df_c.empty:
            st.info("Aucune donnée pour cette sélection.")
        elif vue_c == "Évolution dans le temps":
            # Construire labels d'affichage "Prénom NOM" pour les traces
            if "prenom" in df_c.columns:
                df_c = df_c.copy()
                df_c["label_joueur"] = df_c.apply(
                    lambda r: f"{r['prenom']} {r['nom']}" if pd.notna(r.get("prenom")) else r["nom"], axis=1
                )
                color_col = "label_joueur"
            else:
                color_col = "nom"

            fig = px.line(
                df_c.sort_values("date"),
                x="date", y=metrique_col_c,
                color=color_col,
                markers=True,
                labels={"date": "", metrique_col_c: metrique_label_c, color_col: "Joueur"},
                color_discrete_sequence=RADAR_COLORS[:len(joueurs_sel_c)],
            )
            fig.update_layout(
                legend=dict(orientation="h", y=1.12, font_color="#1a3a5c"),
                height=420,
                **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "height"},
            )
            st.plotly_chart(fig, use_container_width=True)

        else:
            # Agrégé saison — utiliser labels "Prénom NOM"
            if "prenom" in df_c.columns:
                df_c = df_c.copy()
                df_c["label_joueur"] = df_c.apply(
                    lambda r: f"{r['prenom']} {r['nom']}" if pd.notna(r.get("prenom")) else r["nom"], axis=1
                )
                group_col = "label_joueur"
            else:
                group_col = "nom"

            df_agg_c = (
                df_c.groupby(group_col)[metrique_col_c]
                .mean()
                .reset_index()
                .rename(columns={group_col: "Joueur", metrique_col_c: metrique_label_c})
                .sort_values(metrique_label_c, ascending=False)
            )
            fig = px.bar(
                df_agg_c,
                x="Joueur", y=metrique_label_c,
                color="Joueur",
                color_discrete_sequence=RADAR_COLORS,
                labels={"Joueur": ""},
                text=metrique_label_c,
            )
            fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig.update_layout(
                showlegend=False, height=400,
                **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "height"},
            )
            st.plotly_chart(fig, use_container_width=True)
