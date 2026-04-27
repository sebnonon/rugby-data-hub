"""
Dashboard Performance équipe — Rugby Data Hub
Vue équipe par match : GPS & codage match agrégés, mêlées, touches.
Toggle Match / Moyenne pour basculer entre un match précis et la moyenne saison.
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
    page_title="Performance équipe",
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
    .section-gps    { color: #0a7ab0; border-left: 3px solid #0a7ab0; background: #0a7ab012; }
    .section-tech   { color: #E69F00; border-left: 3px solid #E69F00; background: #E69F0010; }
    .section-melee  { color: #009E73; border-left: 3px solid #009E73; background: #009E7312; }
    .section-touche { color: #CC79A7; border-left: 3px solid #CC79A7; background: #CC79A712; }

    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stCaption { color: #5a8aaa; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
    [data-testid="stExpander"] { background-color: #ffffff; border: 1px solid #c0d8ea; border-radius: 8px; }
    .block-container { padding-top: 1.5rem !important; }
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
def load_perf_match():
    c = get_client()
    cols = (
        "joueur_id, match_id, "
        "total_distance, max_speed, sprints, sprint_distance, "
        "high_speed_running_absolute, hml_distance, accelerations_absolute, decelerations_absolute, "
        "collision_load, collisions, dynamic_stress_load, metabolic_distance_absolute, "
        "passes_total, passes_positif, plaquages_total, plaquages_positif, "
        "porteur_total, soutiens_total, contacts_total, grattages_total, "
        "essais_total, minutes_jouees, "
        "joueur(nom, prenom, poste_principal), "
        "match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, journee, session_title)"
    )
    res = c.table("perf_match").select(cols).execute()
    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":          r["joueur_id"],
            "match_id":           r["match_id"],
            "nom":                r["joueur"]["nom"] if r["joueur"] else None,
            "prenom":             r["joueur"]["prenom"] if r["joueur"] else None,
            "poste":              r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":               r["match"]["date"] if r["match"] else None,
            "adversaire":         r["match"]["adversaire"] if r["match"] else None,
            "adversaire_complet": r["match"]["adversaire_nom_complet"] if r["match"] else None,
            "score_rec":          r["match"]["score_rec"] if r["match"] else None,
            "score_adv":          r["match"]["score_adv"] if r["match"] else None,
            "journee":            r["match"]["journee"] if r["match"] else None,
            "match_titre":        r["match"]["session_title"] if r["match"] else None,
            "total_distance":     r["total_distance"],
            "max_speed":          r["max_speed"],
            "sprints":            r["sprints"],
            "sprint_distance":    r["sprint_distance"],
            "hsr":                r["high_speed_running_absolute"],
            "hml_distance":       r["hml_distance"],
            "accelerations":      r["accelerations_absolute"],
            "decelerations":      r["decelerations_absolute"],
            "collision_load":     r["collision_load"],
            "collisions":         r["collisions"],
            "dsl":                r["dynamic_stress_load"],
            "metabolic_distance": r["metabolic_distance_absolute"],
            "passes_total":       r["passes_total"],
            "passes_positif":     r["passes_positif"],
            "plaquages_total":    r["plaquages_total"],
            "plaquages_positif":  r["plaquages_positif"],
            "porteur_total":      r["porteur_total"],
            "soutiens_total":     r["soutiens_total"],
            "contacts_total":     r["contacts_total"],
            "grattages_total":    r["grattages_total"],
            "essais_total":       r["essais_total"],
            "minutes_jouees":     r["minutes_jouees"],
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


@st.cache_data(ttl=300)
def load_melee():
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
            "joueur_id":   r["joueur_id"],
            "match_id":    r["match_id"],
            "scrum_num":   r["scrum_num"],
            "mi_temps":    r["mi_temps"],
            "nom":         r["joueur"]["nom"] if r["joueur"] else None,
            "poste":       r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":        r["match"]["date"] if r["match"] else None,
            "adversaire":  r["match"]["adversaire"] if r["match"] else None,
            "match_titre": r["match"]["session_title"] if r["match"] else None,
            "impact":      to_float(r["impact"]),
            "scrum_load":  to_float(r["scrum_load"]),
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values(["date", "match_id", "scrum_num"])


@st.cache_data(ttl=300)
def load_touche():
    c = get_client()
    res = (
        c.table("touche")
        .select("match_id, equipe, est_rec, resultat, alignement, sortie, zone, start_sec, "
                "match(date, adversaire, session_title)")
        .execute()
    )
    rows = []
    for r in res.data:
        rows.append({
            "match_id":    r["match_id"],
            "equipe":      r["equipe"],
            "est_rec":     r["est_rec"],
            "resultat":    r["resultat"],
            "alignement":  r["alignement"],
            "sortie":      r["sortie"],
            "zone":        r["zone"],
            "start_sec":   r["start_sec"],
            "date":        r["match"]["date"] if r["match"] else None,
            "adversaire":  r["match"]["adversaire"] if r["match"] else None,
            "match_titre": r["match"]["session_title"] if r["match"] else None,
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def kpis_row(items: list[tuple[str, str]]):
    """Affiche une ligne de KPI (label, valeur) sous forme de st.metric, 4 par ligne."""
    for i in range(0, len(items), 4):
        chunk = items[i:i+4]
        cols = st.columns(4)
        for j, (lbl, val) in enumerate(chunk):
            cols[j].metric(lbl, val)


def make_match_label(row: pd.Series) -> str:
    date_str = row["date"].strftime("%d/%m/%y") if pd.notna(row["date"]) else "?"
    adv = row.get("adversaire_complet") or row.get("adversaire") or "?"
    return f"{adv} · {date_str}"


# ── En-tête ───────────────────────────────────────────────────────────────────
df_perf = load_perf_match()
df_melee = load_melee()
df_touche = load_touche()

# Construire la liste des matchs à partir de perf_match (source la plus complète)
df_matchs_ref = (
    df_perf[["match_id", "date", "adversaire", "adversaire_complet", "score_rec", "score_adv", "journee"]]
    .drop_duplicates(subset=["match_id"])
    .dropna(subset=["date"])
    .sort_values("date")
)
df_matchs_ref["label"] = df_matchs_ref.apply(make_match_label, axis=1)
matchs_labels = df_matchs_ref.sort_values("date", ascending=False)["label"].tolist()
label_to_mid  = dict(zip(df_matchs_ref["label"], df_matchs_ref["match_id"]))

col_logo, col_titre, col_match, col_vue = st.columns([1, 3, 4, 2], gap="small")

with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.markdown('<div style="margin-top: 20px"></div>', unsafe_allow_html=True)
        st.image(str(logo_path), width=80)

with col_titre:
    st.markdown('<div style="margin-top: 22px"></div>', unsafe_allow_html=True)
    st.markdown("## Performance équipe")

with col_match:
    match_sel = st.selectbox(
        "Match", matchs_labels if matchs_labels else ["—"],
        key="eq_match", disabled=not matchs_labels,
    )

with col_vue:
    st.markdown('<div style="height:27px"></div>', unsafe_allow_html=True)
    vue = st.radio(
        "Vue", ["🏉 Match", "📊 Moyenne"],
        horizontal=True, label_visibility="collapsed", key="eq_vue",
    )

# Encadrés info match
if matchs_labels and vue == "🏉 Match":
    _row_m = df_matchs_ref[df_matchs_ref["label"] == match_sel].iloc[0]
    _spacer, _c1, _c2, _c3 = st.columns([4, 2, 2, 2])
    sr, sa = _row_m.get("score_rec"), _row_m.get("score_adv")
    try:
        _score = f"{int(sr)} — {int(sa)}" if (pd.notna(sr) and pd.notna(sa)) else "—"
    except (TypeError, ValueError):
        _score = "—"
    _journee = _row_m.get("journee")
    try:
        _journee_str = f"J{int(_journee)}" if pd.notna(_journee) else "—"
    except (TypeError, ValueError):
        _journee_str = "—"
    _n_joueurs = len(df_perf[df_perf["match_id"] == label_to_mid.get(match_sel, -1)])
    _c1.metric("Score", _score)
    _c2.metric("Journée", _journee_str)
    _c3.metric("Joueurs GPS", str(_n_joueurs) if _n_joueurs > 0 else "—")

st.divider()

# ── Sélection du match courant ────────────────────────────────────────────────
if matchs_labels:
    mid_sel = label_to_mid.get(match_sel, -1)
    df_match_perf  = df_perf[df_perf["match_id"] == mid_sel]
    df_match_melee = df_melee[df_melee["match_id"] == mid_sel]
    df_match_touche = df_touche[(df_touche["match_id"] == mid_sel) & (df_touche["est_rec"] == 1)]
else:
    df_match_perf = df_perf.iloc[0:0]
    df_match_melee = df_melee.iloc[0:0]
    df_match_touche = df_touche.iloc[0:0]

df_nos_touches = df_touche[df_touche["est_rec"] == 1]

# ══════════════════════════════════════════════════════════════════════════════
# SECTION A — GPS & CODAGE MATCH
# ══════════════════════════════════════════════════════════════════════════════

section("GPS & Codage match — équipe", "section-gps")

GPS_AGG = {
    "Distance totale (m)": ("total_distance", "sum", 0, " m"),
    "Vitesse max (km/h)":  ("max_speed",      "max", 1, " km/h"),
    "Sprints":             ("sprints",         "sum", 0, ""),
    "HSR (m)":             ("hsr",             "sum", 0, " m"),
    "Charge DSL":          ("dsl",             "sum", 1, ""),
    "Accélérations":       ("accelerations",   "sum", 0, ""),
    "Dist. métabolique":   ("metabolic_distance", "sum", 0, " m"),
}

TECH_AGG = {
    "Passes":     ("passes_total",    "sum", 0, ""),
    "Plaquages":  ("plaquages_total", "sum", 0, ""),
    "Porteurs":   ("porteur_total",   "sum", 0, ""),
    "Soutiens":   ("soutiens_total",  "sum", 0, ""),
    "Contacts":   ("contacts_total",  "sum", 0, ""),
    "Essais":     ("essais_total",    "sum", 0, ""),
}


def _agg_perf(df: pd.DataFrame, agg_map: dict) -> list[tuple[str, str]]:
    result = []
    for label, (col, func, dec, suf) in agg_map.items():
        if col not in df.columns:
            result.append((label, "—"))
            continue
        series = df[col].dropna()
        if series.empty:
            result.append((label, "—"))
            continue
        val = getattr(series, func)()
        result.append((label, fmt(val, decimals=dec, suffix=suf)))
    return result


if vue == "🏉 Match":
    if df_match_perf.empty:
        st.info("Aucune donnée GPS pour ce match.")
    else:
        st.markdown("##### GPS")
        kpis_row(_agg_perf(df_match_perf, GPS_AGG))
        st.markdown("##### Codage vidéo")
        kpis_row(_agg_perf(df_match_perf, TECH_AGG))

        # Tableau des joueurs
        with st.expander("Détail par joueur"):
            cols_show = ["nom", "prenom", "poste", "minutes_jouees",
                         "total_distance", "max_speed", "sprints", "hsr", "dsl",
                         "passes_total", "plaquages_total", "porteur_total", "essais_total"]
            cols_show = [c for c in cols_show if c in df_match_perf.columns]
            df_disp = df_match_perf[cols_show].copy()
            df_disp["Joueur"] = df_disp.apply(
                lambda r: f"{r['prenom']} {r['nom']}" if "prenom" in r and pd.notna(r.get("prenom")) else r["nom"],
                axis=1,
            )
            rename_map = {
                "Joueur": "Joueur", "poste": "Poste", "minutes_jouees": "Min",
                "total_distance": "Distance (m)", "max_speed": "Vmax (km/h)",
                "sprints": "Sprints", "hsr": "HSR (m)", "dsl": "DSL",
                "passes_total": "Passes", "plaquages_total": "Plaquages",
                "porteur_total": "Porteurs", "essais_total": "Essais",
            }
            cols_final = [c for c in ["Joueur", "poste", "minutes_jouees",
                                       "total_distance", "max_speed", "sprints", "hsr", "dsl",
                                       "passes_total", "plaquages_total", "porteur_total", "essais_total"]
                          if c in df_disp.columns or c == "Joueur"]
            df_disp = df_disp[["Joueur"] + [c for c in cols_final if c != "Joueur" and c in df_disp.columns]]
            df_disp = df_disp.rename(columns=rename_map).sort_values(
                "Min" if "Min" in df_disp.columns else "Joueur", ascending=False
            )
            st.dataframe(df_disp, use_container_width=True, hide_index=True)

else:
    # Moyenne — agrégation par match puis moyenne des matchs
    df_par_match = (
        df_perf.groupby("match_id")
        .agg(
            total_distance=("total_distance", "sum"),
            max_speed=("max_speed", "max"),
            sprints=("sprints", "sum"),
            hsr=("hsr", "sum"),
            dsl=("dsl", "sum"),
            accelerations=("accelerations", "sum"),
            metabolic_distance=("metabolic_distance", "sum"),
            passes_total=("passes_total", "sum"),
            plaquages_total=("plaquages_total", "sum"),
            porteur_total=("porteur_total", "sum"),
            soutiens_total=("soutiens_total", "sum"),
            contacts_total=("contacts_total", "sum"),
            essais_total=("essais_total", "sum"),
        )
        .reset_index()
    )
    df_par_match = df_par_match.merge(
        df_matchs_ref[["match_id", "date", "label"]], on="match_id", how="left"
    ).sort_values("date")

    moyennes_gps = {
        "Distance moy. (m)":  ("total_distance",    1, " m"),
        "Vmax équipe (km/h)":  ("max_speed",         1, " km/h"),
        "Sprints moy.":        ("sprints",            0, ""),
        "HSR moy. (m)":        ("hsr",               0, " m"),
        "DSL moy.":            ("dsl",               1, ""),
        "Accélérations moy.":  ("accelerations",     0, ""),
    }
    moyennes_tech = {
        "Passes moy.":    ("passes_total",    0, ""),
        "Plaquages moy.": ("plaquages_total", 0, ""),
        "Porteurs moy.":  ("porteur_total",   0, ""),
        "Essais moy.":    ("essais_total",    0, ""),
    }

    st.markdown("##### GPS")
    kpis_row([
        (lbl, fmt(df_par_match[col].mean(), decimals=dec, suffix=suf))
        for lbl, (col, dec, suf) in moyennes_gps.items()
        if col in df_par_match.columns
    ])
    st.markdown("##### Codage vidéo")
    kpis_row([
        (lbl, fmt(df_par_match[col].mean(), decimals=dec, suffix=suf))
        for lbl, (col, dec, suf) in moyennes_tech.items()
        if col in df_par_match.columns
    ])

    st.divider()

    # Bar chart distance totale par match
    if not df_par_match.empty and "total_distance" in df_par_match.columns:
        col_l, col_r = st.columns(2)
        with col_l:
            fig = px.bar(
                df_par_match, x="label", y="total_distance",
                labels={"label": "", "total_distance": "Distance totale (m)"},
                color_discrete_sequence=["#56B4E9"],
                text="total_distance",
            )
            fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            if "essais_total" in df_par_match.columns:
                fig2 = px.bar(
                    df_par_match, x="label", y="essais_total",
                    labels={"label": "", "essais_total": "Essais"},
                    color_discrete_sequence=["#E69F00"],
                    text="essais_total",
                )
                fig2.update_traces(texttemplate="%{text:.0f}", textposition="outside")
                fig2.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
                st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION B — MÊLÉES ÉQUIPE
# ══════════════════════════════════════════════════════════════════════════════

section("Mêlées — équipe", "section-melee")

if df_melee.empty:
    st.info("Aucune donnée de mêlée disponible.")
else:
    # Agrégation par match (déduplique les joueurs pour chaque scrum)
    df_scrums_all = (
        df_melee.groupby(["match_id", "date", "adversaire", "scrum_num"])
        .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
        .reset_index()
    )
    df_melee_par_match = (
        df_scrums_all.groupby(["match_id", "date", "adversaire"])
        .agg(
            nb_melees=("scrum_num", "count"),
            impact_moy=("impact_moy", "mean"),
            scrum_load_total=("scrum_load_moy", "sum"),
        )
        .reset_index()
        .sort_values("date")
    )
    df_melee_par_match["label"] = (
        df_melee_par_match["date"].dt.strftime("%d/%m") + " · " + df_melee_par_match["adversaire"].fillna("?")
    )
    df_melee_par_match = df_melee_par_match.merge(
        df_matchs_ref[["match_id", "label"]].rename(columns={"label": "label_full"}),
        on="match_id", how="left",
    )

    if vue == "🏉 Match":
        df_match_sc = df_scrums_all[df_scrums_all["match_id"] == mid_sel]
        if df_match_sc.empty:
            st.info("Aucune donnée de mêlée pour ce match.")
        else:
            row_m = df_melee_par_match[df_melee_par_match["match_id"] == mid_sel]
            if not row_m.empty:
                r = row_m.iloc[0]
                kpis_row([
                    ("Nb mêlées", fmt(r["nb_melees"])),
                    ("Impact moy.", fmt(r["impact_moy"], decimals=1)),
                    ("Charge totale", fmt(r["scrum_load_total"], decimals=0)),
                ])

            # Timeline des mêlées du match
            df_tl = (
                df_melee[df_melee["match_id"] == mid_sel]
                .groupby(["scrum_num", "mi_temps"])
                .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
                .reset_index()
                .sort_values(["mi_temps", "scrum_num"])
            )
            df_tl["label"] = df_tl.apply(
                lambda r: f"MT{int(r['mi_temps'])} — M{int(r['scrum_num'])}", axis=1
            )
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df_tl["label"], y=df_tl["impact_moy"],    name="Impact moy.",     marker_color="#56B4E9"))
            fig.add_trace(go.Bar(x=df_tl["label"], y=df_tl["scrum_load_moy"], name="Scrum Load moy.", marker_color="#E69F00"))
            fig.update_layout(
                barmode="group", xaxis_tickangle=-35,
                legend=dict(orientation="h", y=1.12, font_color="#1a3a5c"),
                xaxis_title="", yaxis_title="",
                **PLOTLY_LAYOUT,
            )
            st.plotly_chart(fig, use_container_width=True)

    else:
        kpis_row([
            ("Mêlées totales",     fmt(df_melee_par_match["nb_melees"].sum())),
            ("Impact moy.",        fmt(df_melee_par_match["impact_moy"].mean(), decimals=1)),
            ("Charge moy./match",  fmt(df_melee_par_match["scrum_load_total"].mean(), decimals=0)),
        ])

        col_l, col_r = st.columns(2)
        with col_l:
            fig = px.bar(
                df_melee_par_match, x="label", y="nb_melees",
                labels={"label": "", "nb_melees": "Nb mêlées"},
                color_discrete_sequence=["#56B4E9"],
            )
            fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            fig2 = px.bar(
                df_melee_par_match, x="label", y="impact_moy",
                labels={"label": "", "impact_moy": "Impact moy."},
                color_discrete_sequence=["#E69F00"],
            )
            fig2.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION C — TOUCHES
# ══════════════════════════════════════════════════════════════════════════════

section("Touches — équipe", "section-touche")

if df_nos_touches.empty:
    st.info("Aucune donnée de touche disponible.")
else:
    def _taux_reussite(df: pd.DataFrame) -> str:
        if "resultat" not in df.columns or df.empty:
            return "—"
        reussies = df["resultat"].str.lower().isin(["gagné", "gagne", "won", "win", "r", "1", "oui"]).sum()
        total = len(df)
        if total == 0:
            return "—"
        return f"{reussies}/{total} ({round(reussies/total*100)}%)"

    if vue == "🏉 Match":
        df_t_match = df_match_touche
        if df_t_match.empty:
            st.info("Aucune touche pour ce match.")
        else:
            kpis_row([
                ("Touches jouées", str(len(df_t_match))),
                ("Résultat",       _taux_reussite(df_t_match)),
            ])

            col_l, col_r = st.columns(2)
            with col_l:
                if "zone" in df_t_match.columns and df_t_match["zone"].notna().any():
                    vc = df_t_match["zone"].value_counts().reset_index()
                    vc.columns = ["Zone", "Nb"]
                    fig = px.bar(vc, x="Zone", y="Nb", color="Zone",
                                 color_discrete_sequence=["#CC79A7", "#56B4E9", "#E69F00", "#009E73"],
                                 labels={"Zone": "", "Nb": "Nb touches"})
                    fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
            with col_r:
                if "alignement" in df_t_match.columns and df_t_match["alignement"].notna().any():
                    vc2 = df_t_match["alignement"].value_counts().reset_index()
                    vc2.columns = ["Alignement", "Nb"]
                    fig2 = px.pie(vc2, names="Alignement", values="Nb",
                                  color_discrete_sequence=["#CC79A7", "#56B4E9", "#E69F00", "#009E73", "#F0E442"])
                    fig2.update_layout(paper_bgcolor="#ffffff", font_color="#1a3a5c",
                                       margin=dict(t=20, b=20), height=300)
                    st.plotly_chart(fig2, use_container_width=True)

            if "resultat" in df_t_match.columns and df_t_match["resultat"].notna().any():
                with st.expander("Détail des touches"):
                    cols_disp = [c for c in ["resultat", "alignement", "zone", "sortie"] if c in df_t_match.columns]
                    st.dataframe(df_t_match[cols_disp], use_container_width=True, hide_index=True)

    else:
        # Agrégation par match
        df_touche_pm = (
            df_nos_touches.groupby("match_id")
            .agg(nb_touches=("match_id", "count"))
            .reset_index()
        )
        df_touche_pm = df_touche_pm.merge(
            df_matchs_ref[["match_id", "label"]], on="match_id", how="left"
        ).sort_values("match_id")

        kpis_row([
            ("Touches moy./match",  fmt(df_touche_pm["nb_touches"].mean(), decimals=1)),
            ("Touches totales",     fmt(df_touche_pm["nb_touches"].sum())),
            ("Résultat global",     _taux_reussite(df_nos_touches)),
        ])

        col_l, col_r = st.columns(2)
        with col_l:
            if not df_touche_pm.empty:
                fig = px.bar(
                    df_touche_pm, x="label", y="nb_touches",
                    labels={"label": "", "nb_touches": "Nb touches"},
                    color_discrete_sequence=["#CC79A7"],
                )
                fig.update_layout(xaxis_tickangle=-35, **PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        with col_r:
            if "alignement" in df_nos_touches.columns and df_nos_touches["alignement"].notna().any():
                vc = df_nos_touches["alignement"].value_counts().reset_index()
                vc.columns = ["Alignement", "Nb"]
                fig2 = px.pie(vc, names="Alignement", values="Nb",
                              color_discrete_sequence=["#CC79A7", "#56B4E9", "#E69F00", "#009E73", "#F0E442"])
                fig2.update_layout(paper_bgcolor="#ffffff", font_color="#1a3a5c",
                                   margin=dict(t=20, b=20), height=300)
                st.plotly_chart(fig2, use_container_width=True)
