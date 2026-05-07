"""
Dashboard Performance équipe — Rugby Data Hub
Vue équipe par match : GPS & codage match agrégés, mêlées, touches.
Vue par match : GPS & codage match agrégés, mêlées, touches.
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
        border-radius: 8px;
        padding: 8px 12px;
    }
    [data-testid="stMetricLabel"] {
        color: #2a6080;
        font-size: 0.68rem;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        font-weight: 700;
    }
    [data-testid="stMetricValue"] {
        color: #071626;
        font-weight: 800;
        font-size: 1.1rem !important;
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

    @media (max-width: 1280px) { .block-container { zoom: 0.85; } }
    @media (min-width: 1281px) and (max-width: 1600px) { .block-container { zoom: 0.95; } }
    @media (min-width: 1601px) and (max-width: 1920px) { .block-container { zoom: 1.0; } }
    @media (min-width: 1921px) { .block-container { zoom: 1.1; } }

    [data-testid="stSelectbox"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stCaption { color: #5a8aaa; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
    [data-testid="stExpander"] { background-color: #ffffff; border: 1px solid #c0d8ea; border-radius: 8px; }
    .block-container { padding-top: 3.5rem !important; }
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
        "jap_total, jap_positif, jap_negatif, jap_neutre, "
        "jap_depuis_propre_22m, jap_depuis_mi_terrain, jap_depuis_camp_adverse, "
        "jap_en_touche, jap_camp_adverse, "
        "joueur(nom, prenom, poste_principal), "
        "match(date, adversaire, adversaire_nom_complet, score_rec, score_adv, journee, session_title, "
        "melee_total_rec, melee_positif_rec, melee_negatif_rec, melee_neutre_rec, "
        "possession_rec, possession_adv)"
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
            "plaquages_total":       r["plaquages_total"],
            "plaquages_positif":     r["plaquages_positif"],
            "porteur_total":         r["porteur_total"],
            "soutiens_total":        r["soutiens_total"],
            "contacts_total":        r["contacts_total"],
            "grattages_total":       r["grattages_total"],
            "essais_total":          r["essais_total"],
            "minutes_jouees":        r["minutes_jouees"],
            "jap_total":             r["jap_total"],
            "jap_positif":           r["jap_positif"],
            "jap_negatif":           r["jap_negatif"],
            "jap_neutre":            r["jap_neutre"],
            "jap_depuis_propre_22m": r["jap_depuis_propre_22m"],
            "jap_depuis_mi_terrain": r["jap_depuis_mi_terrain"],
            "jap_depuis_camp_adv":   r["jap_depuis_camp_adverse"],
            "jap_en_touche":         r["jap_en_touche"],
            "jap_camp_adverse":      r["jap_camp_adverse"],
            "melee_positif_rec":     r["match"]["melee_positif_rec"] if r["match"] else None,
            "melee_negatif_rec":     r["match"]["melee_negatif_rec"] if r["match"] else None,
            "melee_neutre_rec":      r["match"]["melee_neutre_rec"]  if r["match"] else None,
            "melee_total_rec":       r["match"]["melee_total_rec"]   if r["match"] else None,
            "possession_rec":        r["match"]["possession_rec"]    if r["match"] else None,
            "possession_adv":        r["match"]["possession_adv"]    if r["match"] else None,
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
    df_perf[[
        "match_id", "date", "adversaire", "adversaire_complet", "score_rec", "score_adv", "journee",
        "melee_positif_rec", "melee_negatif_rec", "melee_neutre_rec", "melee_total_rec",
        "possession_rec", "possession_adv",
    ]]
    .drop_duplicates(subset=["match_id"])
    .dropna(subset=["date"])
    .sort_values("date")
)
df_matchs_ref["label"] = df_matchs_ref.apply(make_match_label, axis=1)
matchs_labels = df_matchs_ref.sort_values("date", ascending=False)["label"].tolist()
label_to_mid  = dict(zip(df_matchs_ref["label"], df_matchs_ref["match_id"]))

st.markdown(
    '<p style="font-weight:700; font-size:1.1rem; color:#071626; margin:0; padding:0.25rem 0 0.5rem 0; text-align:center;">Performance équipe</p>',
    unsafe_allow_html=True,
)

c_match, c_score, c_journee = st.columns([4, 2, 2], gap="small")
with c_match:
    match_sel = st.selectbox(
        "Match", matchs_labels if matchs_labels else ["—"],
        key="eq_match", disabled=not matchs_labels,
    )

if matchs_labels:
    _row_m = df_matchs_ref[df_matchs_ref["label"] == match_sel].iloc[0]
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
    with c_score:
        st.metric("Score", _score)
    with c_journee:
        st.metric("Journée", _journee_str)


# ── Sélection du match courant ────────────────────────────────────────────────
if matchs_labels:
    mid_sel = label_to_mid.get(match_sel, -1)
    df_match_perf  = df_perf[df_perf["match_id"] == mid_sel]
    df_match_melee = df_melee[df_melee["match_id"] == mid_sel]
    df_match_touche = df_touche[(df_touche["match_id"] == mid_sel) & (df_touche["est_rec"] == 1)]
else:
    mid_sel = -1
    df_match_perf = df_perf.iloc[0:0]
    df_match_melee = df_melee.iloc[0:0]
    df_match_touche = df_touche.iloc[0:0]

df_nos_touches = df_touche[df_touche["est_rec"] == 1]

# ── KPIs plaquages & passes ───────────────────────────────────────────────────
if not df_match_perf.empty:
    pla_total = df_match_perf["plaquages_total"].dropna().sum()
    pla_pos   = df_match_perf["plaquages_positif"].dropna().sum()
    pla_pct   = f"{round(pla_pos / pla_total * 100)} %" if pla_total > 0 else "—"
    pas_total = df_match_perf["passes_total"].dropna().sum()
    pas_pos   = df_match_perf["passes_positif"].dropna().sum()
    pas_pct   = f"{round(pas_pos / pas_total * 100)} %" if pas_total > 0 else "—"
    _ka, _kb = st.columns(2)
    _ka.metric("Plaquages", fmt(pla_total))
    _kb.metric("Réussite plaquages", pla_pct)
    _kc, _kd = st.columns(2)
    _kc.metric("Passes", fmt(pas_total))
    _kd.metric("Réussite passes", pas_pct)
    st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTIONS B & C — MÊLÉES + TOUCHES CÔTE À CÔTE
# ══════════════════════════════════════════════════════════════════════════════

# Pré-calcul mêlées (hors colonnes Streamlit)
def _is_gagnee(val):
    return str(val).lower() in ["gagnée", "gagné", "gagne", "won", "win", "r", "1", "oui"] if pd.notna(val) else False

def _taux_reussite(df: pd.DataFrame) -> str:
    if "resultat" not in df.columns or df.empty:
        return "—"
    reussies = df["resultat"].str.lower().isin(["gagnée", "gagné", "gagne", "won", "win", "r", "1", "oui"]).sum()
    total = len(df)
    return f"{reussies}/{total} ({round(reussies/total*100)}%)" if total > 0 else "—"

if not df_melee.empty:
    df_scrums_all = (
        df_melee.groupby(["match_id", "date", "adversaire", "scrum_num"])
        .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
        .reset_index()
    )
    df_melee_par_match = (
        df_scrums_all.groupby(["match_id", "date", "adversaire"])
        .agg(nb_melees=("scrum_num", "count"), impact_moy=("impact_moy", "mean"), scrum_load_total=("scrum_load_moy", "sum"))
        .reset_index().sort_values("date")
    )
    df_melee_par_match["label"] = df_melee_par_match["date"].dt.strftime("%d/%m") + " · " + df_melee_par_match["adversaire"].fillna("?")
    df_melee_par_match = df_melee_par_match.merge(
        df_matchs_ref[["match_id", "label"]].rename(columns={"label": "label_full"}), on="match_id", how="left",
    )
    _row_match = df_matchs_ref[df_matchs_ref["match_id"] == mid_sel]
    mel_pie_data = None
    if not _row_match.empty:
        _rm = _row_match.iloc[0]
        mel_pos, mel_neg, mel_neu = _rm.get("melee_positif_rec") or 0, _rm.get("melee_negatif_rec") or 0, _rm.get("melee_neutre_rec") or 0
        if mel_pos + mel_neg + mel_neu > 0:
            mel_pie_data = pd.DataFrame({"Issue": ["Gagnée", "Perdue", "Neutre"], "Nb": [mel_pos, mel_neg, mel_neu]})

col_melee, col_touche = st.columns(2, gap="medium")

# ── Colonne Mêlées ────────────────────────────────────────────────────────────
with col_melee:
    if df_melee.empty:
        st.info("Aucune donnée de mêlée disponible.")
    else:
        if mel_pie_data is not None:
            fig_pie = px.pie(
                mel_pie_data, names="Issue", values="Nb", color="Issue",
                color_discrete_map={"Gagnée": "#009E73", "Perdue": "#E05C5C", "Neutre": "#aaaaaa"},
            )
            fig_pie.update_layout(
                paper_bgcolor="#ffffff", font_color="#1a3a5c",
                title=dict(text="Mêlées", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")),
                margin=dict(t=40, b=10, l=10, r=10), height=300,
                legend=dict(orientation="h", y=-0.1),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Données mêlées (codage vidéo) non disponibles.")

        df_match_sc = df_scrums_all[df_scrums_all["match_id"] == mid_sel]
        if not df_match_sc.empty:
            row_m = df_melee_par_match[df_melee_par_match["match_id"] == mid_sel]
            if not row_m.empty:
                r = row_m.iloc[0]
                kpis_row([
                    ("Nb mêlées GPS", fmt(r["nb_melees"])),
                    ("Impact moy.",   fmt(r["impact_moy"], decimals=1)),
                    ("Charge totale", fmt(r["scrum_load_total"], decimals=0)),
                ])
            df_tl = (
                df_melee[df_melee["match_id"] == mid_sel]
                .groupby(["scrum_num", "mi_temps"])
                .agg(impact_moy=("impact", "mean"), scrum_load_moy=("scrum_load", "mean"))
                .reset_index().sort_values(["mi_temps", "scrum_num"])
            )
            df_tl["label"] = df_tl.apply(lambda r: f"MT{int(r['mi_temps'])} — M{int(r['scrum_num'])}", axis=1)
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df_tl["label"], y=df_tl["impact_moy"],     name="Impact moy.",     marker_color="#56B4E9"))
            fig.add_trace(go.Bar(x=df_tl["label"], y=df_tl["scrum_load_moy"], name="Scrum Load moy.", marker_color="#E69F00"))
            fig.update_layout(barmode="group", xaxis_tickangle=-35,
                              title=dict(text="Impact & Charge par mêlée", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")),
                              legend=dict(orientation="h", y=1.18, font_color="#1a3a5c"),
                              xaxis_title="", yaxis_title="", **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

# ── Colonne Touches ───────────────────────────────────────────────────────────
with col_touche:
    if df_nos_touches.empty:
        st.info("Aucune donnée de touche disponible.")
    else:
        df_t_match = df_match_touche
        if df_t_match.empty:
            st.info("Aucune touche pour ce match.")
        else:
            if "zone" in df_t_match.columns and df_t_match["zone"].notna().any():
                df_tz = df_t_match[df_t_match["zone"].notna()].copy()
                df_tz["issue"] = df_tz["resultat"].apply(lambda v: "Gagnée" if _is_gagnee(v) else "Perdue")
                df_zone = df_tz.groupby(["zone", "issue"]).size().reset_index(name="nb")
                fig = px.bar(df_zone, x="zone", y="nb", color="issue",
                             color_discrete_map={"Gagnée": "#009E73", "Perdue": "#E05C5C"},
                             barmode="stack", labels={"zone": "", "nb": "Nb touches", "issue": ""})
                fig.update_layout(**PLOTLY_LAYOUT,
                                  title=dict(text="Touches par zone", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")),
                                  legend=dict(orientation="h", y=1.18, font_color="#1a3a5c"))
                st.plotly_chart(fig, use_container_width=True)
            if "alignement" in df_t_match.columns and df_t_match["alignement"].notna().any():
                vc2 = df_t_match["alignement"].value_counts().reset_index()
                vc2.columns = ["Alignement", "Nb"]
                fig2 = px.pie(vc2, names="Alignement", values="Nb",
                              color_discrete_sequence=["#CC79A7", "#56B4E9", "#E69F00", "#009E73", "#F0E442"])
                fig2.update_layout(paper_bgcolor="#ffffff", font_color="#1a3a5c",
                                   title=dict(text="Alignements", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")),
                                   margin=dict(t=40, b=20), height=300)
                st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION D — JEUX AU PIED
# ══════════════════════════════════════════════════════════════════════════════


if df_match_perf.empty:
    st.info("Aucune donnée de jeu au pied pour ce match.")
else:
    jap_t   = int(df_match_perf["jap_total"].dropna().sum())
    jap_pos = int(df_match_perf["jap_positif"].dropna().sum())
    jap_neg = int(df_match_perf["jap_negatif"].dropna().sum())
    jap_neu = int(df_match_perf["jap_neutre"].dropna().sum())
    jap_tch = int(df_match_perf["jap_en_touche"].dropna().sum())
    jap_pct_tch = f"{round(jap_tch / jap_t * 100)} %" if jap_t > 0 else "—"

    kpis_row([
        ("Jeux au pied", fmt(jap_t)),
        ("En touche", fmt(jap_tch)),
        ("% en touche", jap_pct_tch),
    ])

    if jap_t > 0:
        col_l, col_r = st.columns(2)

        with col_l:
            # Camembert qualité (positif / négatif / neutre)
            if jap_pos + jap_neg + jap_neu > 0:
                df_qual = pd.DataFrame({
                    "Issue": ["Positif", "Négatif", "Neutre"],
                    "Nb":    [jap_pos,   jap_neg,   jap_neu],
                })
                fig_q = px.pie(
                    df_qual, names="Issue", values="Nb",
                    color="Issue",
                    color_discrete_map={"Positif": "#009E73", "Négatif": "#E05C5C", "Neutre": "#aaaaaa"},
                )
                fig_q.update_layout(
                    paper_bgcolor="#ffffff", font_color="#1a3a5c",
                    title=dict(text="Qualité des jeux au pied", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")),
                    margin=dict(t=40, b=10, l=10, r=10), height=300,
                    legend=dict(orientation="h", y=-0.1),
                )
                st.plotly_chart(fig_q, use_container_width=True)

        with col_r:
            # Bar zones d'origine
            zones_data = {
                "Propre 22m":  int(df_match_perf["jap_depuis_propre_22m"].dropna().sum()),
                "Mi-terrain":  int(df_match_perf["jap_depuis_mi_terrain"].dropna().sum()),
                "Camp adverse": int(df_match_perf["jap_depuis_camp_adv"].dropna().sum()),
            }
            df_zones = pd.DataFrame(zones_data.items(), columns=["Zone", "Nb"])
            df_zones = df_zones[df_zones["Nb"] > 0]
            if not df_zones.empty:
                fig_z = px.bar(
                    df_zones, x="Zone", y="Nb",
                    color="Zone",
                    color_discrete_sequence=["#56B4E9", "#E69F00", "#CC79A7"],
                    labels={"Zone": "", "Nb": "Nb jeux au pied"},
                )
                fig_z.update_layout(**PLOTLY_LAYOUT, showlegend=False,
                                    title=dict(text="Zones d'origine", x=0.5, xanchor="center", font=dict(size=13, color="#1a3a5c")))
                st.plotly_chart(fig_z, use_container_width=True)
