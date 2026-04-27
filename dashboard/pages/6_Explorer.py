"""
Page Explorateur de stats — Rugby Data Hub (staff)
Trois modes : Classement · Comparaison joueurs · Tableau libre
"""

import sys
from pathlib import Path
from datetime import date as date_type
import streamlit as st
from PIL import Image
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from supabase_client import get_client

# ── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Rugby Data Hub — Explorateur", page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg"), layout="wide")

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
    [data-testid="stSelectbox"] label,
    [data-testid="stMultiSelect"] label { color: #7ab8d8; }
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
    margin=dict(t=20),
)

# ── Définition des métriques disponibles par source ───────────────────────────

METRIQUES_MATCH = {
    # GPS
    "Distance totale (m)":          "total_distance",
    "Vitesse max (km/h)":           "max_speed",
    "Sprints":                      "sprints",
    "Distance sprint (m)":          "sprint_distance",
    "HSR (m)":                      "high_speed_running_absolute",
    "Distance HML (m)":             "hml_distance",
    "Accélérations":                "accelerations_absolute",
    "Décélérations":                "decelerations_absolute",
    "Charge de collision":          "collision_load",
    "Nb collisions GPS":            "collisions",
    "DSL":                          "dynamic_stress_load",
    "Distance métabolique (m)":     "metabolic_distance_absolute",
    # Actions
    "Passes (total)":               "passes_total",
    "Passes (positives)":           "passes_positif",
    "Plaquages (total)":            "plaquages_total",
    "Plaquages (positifs)":         "plaquages_positif",
    "Porteur (total)":              "porteur_total",
    "Soutiens (total)":             "soutiens_total",
    "Contacts (total)":             "contacts_total",
    "Grattages (total)":            "grattages_total",
    "Essais":                       "essais_total",
    "Minutes jouées":               "minutes_jouees",
}

METRIQUES_ENTR = {
    "Distance totale (m)":          "total_distance",
    "Vitesse max (km/h)":           "max_speed",
    "Sprints":                      "sprints",
    "Distance sprint (m)":          "sprint_distance",
    "HSR (m)":                      "high_speed_running_absolute",
    "Distance HML (m)":             "hml_distance",
    "Accélérations":                "accelerations_absolute",
    "Décélérations":                "decelerations_absolute",
    "DSL":                          "dynamic_stress_load",
    "Distance métabolique (m)":     "metabolic_distance_absolute",
    "Vitesse moy. (m/min)":         "distance_per_min",
}

METRIQUES_COL = {
    "Nb collisions":                "_count",
    "Charge totale":                "collision_load_sum",
    "Charge moy. / collision":      "collision_load_mean",
    "Time to feet moy. (s)":        "time_to_feet_mean",
    "Post-collision accél. moy.":   "post_collision_accel_mean",
}

SOURCES = {
    "Matchs (GPS + actions)":    "match",
    "Entraînements":             "entr",
    "Collisions":                "col",
}

AGGREGATIONS = {"Moyenne": "mean", "Total": "sum", "Maximum": "max"}

# ── Chargement des données ────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_match():
    c = get_client()
    cols = (
        "joueur_id, match_id, total_distance, max_speed, sprints, sprint_distance, "
        "high_speed_running_absolute, hml_distance, accelerations_absolute, decelerations_absolute, "
        "collision_load, collisions, dynamic_stress_load, metabolic_distance_absolute, "
        "distance_per_min, "
        "passes_total, passes_positif, plaquages_total, plaquages_positif, "
        "porteur_total, soutiens_total, contacts_total, grattages_total, "
        "essais_total, minutes_jouees, "
        "joueur(nom, poste_principal), match(date, adversaire, session_title)"
    )
    res = c.table("perf_match").select(cols).execute()
    rows = []
    for r in res.data:
        row = {k: r[k] for k in r if k not in ("joueur", "match")}
        row["nom"]          = r["joueur"]["nom"] if r["joueur"] else None
        row["poste"]        = r["joueur"]["poste_principal"] if r["joueur"] else None
        row["date"]         = r["match"]["date"] if r["match"] else None
        row["adversaire"]   = r["match"]["adversaire"] if r["match"] else None
        row["match_titre"]  = r["match"]["session_title"] if r["match"] else None
        rows.append(row)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


@st.cache_data(ttl=300)
def load_entr():
    c = get_client()
    cols = (
        "joueur_id, date, session_type, seance_type, "
        "total_distance, max_speed, sprints, sprint_distance, "
        "high_speed_running_absolute, hml_distance, accelerations_absolute, decelerations_absolute, "
        "dynamic_stress_load, metabolic_distance_absolute, distance_per_min, "
        "joueur(nom, poste_principal)"
    )
    res = c.table("perf_entrainement").select(cols).execute()
    rows = []
    for r in res.data:
        row = {k: r[k] for k in r if k not in ("joueur",)}
        row["nom"]   = r["joueur"]["nom"] if r["joueur"] else None
        row["poste"] = r["joueur"]["poste_principal"] if r["joueur"] else None
        rows.append(row)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


@st.cache_data(ttl=300)
def load_col():
    c = get_client()
    res = (
        c.table("collision")
        .select("joueur_id, match_id, collision_load, time_to_feet, post_collision_accel, "
                "joueur(nom, poste_principal), match(date, adversaire)")
        .execute()
    )
    rows = []
    for r in res.data:
        rows.append({
            "joueur_id":            r["joueur_id"],
            "match_id":             r["match_id"],
            "collision_load":       r["collision_load"],
            "time_to_feet":         r["time_to_feet"],
            "post_collision_accel": r["post_collision_accel"],
            "nom":                  r["joueur"]["nom"] if r["joueur"] else None,
            "poste":                r["joueur"]["poste_principal"] if r["joueur"] else None,
            "date":                 r["match"]["date"] if r["match"] else None,
            "adversaire":           r["match"]["adversaire"] if r["match"] else None,
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # Agrégation par joueur/match
    agg = (
        df.groupby(["joueur_id", "match_id", "nom", "poste", "date", "adversaire"])
        .agg(
            _count=("collision_load", "count"),
            collision_load_sum=("collision_load", "sum"),
            collision_load_mean=("collision_load", "mean"),
            time_to_feet_mean=("time_to_feet", "mean"),
            post_collision_accel_mean=("post_collision_accel", "mean"),
        )
        .reset_index()
    )
    return agg.sort_values("date")


def get_df(source_key: str) -> pd.DataFrame:
    if source_key == "match":
        return load_match()
    if source_key == "entr":
        return load_entr()
    return load_col()


def get_metriques(source_key: str) -> dict:
    if source_key == "match":
        return METRIQUES_MATCH
    if source_key == "entr":
        return METRIQUES_ENTR
    return METRIQUES_COL


def slider_periode(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Affiche un slider de plage de dates et retourne le DataFrame filtré."""
    if "date" not in df.columns or df["date"].isna().all():
        return df
    d_min = df["date"].min().date()
    d_max = df["date"].max().date()
    d_from, d_to = st.slider(
        "Période",
        min_value=d_min, max_value=d_max,
        value=(d_min, d_max),
        format="DD/MM/YY",
        key=key,
    )
    return df[(df["date"].dt.date >= d_from) & (df["date"].dt.date <= d_to)]


# ── Titre ─────────────────────────────────────────────────────────────────────
col_logo, col_titre = st.columns([1, 6])
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.image(str(logo_path), width=140)
with col_titre:
    st.title("Rugby Data Hub — Explorateur de stats")
    st.caption("Outil d'analyse staff · Classement · Comparaison · Tableau")

st.divider()

tab_classement, tab_comparaison, tab_tableau = st.tabs([
    "🏆  Classement", "⚖️  Comparaison joueurs", "📋  Tableau libre"
])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — CLASSEMENT
# ══════════════════════════════════════════════════════════════════════════════
with tab_classement:
    st.subheader("Classement joueurs")

    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
    with c1:
        source_label = st.selectbox("Source", list(SOURCES.keys()), key="cl_source")
        source_key   = SOURCES[source_label]
    metriques = get_metriques(source_key)
    with c2:
        metrique_label = st.selectbox("Métrique", list(metriques.keys()), key="cl_metrique")
        metrique_col   = metriques[metrique_label]
    with c3:
        agg_label = st.selectbox("Agrégation", list(AGGREGATIONS.keys()), key="cl_agg")
        agg_func  = AGGREGATIONS[agg_label]
    with c4:
        top_n = st.number_input("Top N", min_value=3, max_value=30, value=10, step=1, key="cl_n")

    df_src = get_df(source_key)
    df_f = slider_periode(df_src, key="cl_periode")

    if metrique_col not in df_f.columns:
        st.warning(f"Métrique `{metrique_col}` absente des données.")
    else:
        df_agg = (
            df_f.groupby("nom")[metrique_col]
            .agg(agg_func)
            .dropna()
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index()
            .rename(columns={"nom": "Joueur", metrique_col: metrique_label})
        )

        if df_agg.empty:
            st.info("Aucune donnée pour ces critères.")
        else:
            fig = px.bar(
                df_agg.sort_values(metrique_label),
                x=metrique_label, y="Joueur",
                orientation="h",
                color=metrique_label,
                color_continuous_scale=[[0, "#0072B2"], [1, "#56B4E9"]],
                labels={"Joueur": ""},
                text=metrique_label,
            )
            fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig.update_layout(
                coloraxis_showscale=False,
                height=max(350, top_n * 38),
                **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "height"},
            )
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Tableau"):
                st.dataframe(
                    df_agg.sort_values(metrique_label, ascending=False)
                    .reset_index(drop=True)
                    .assign(**{"#": lambda x: range(1, len(x) + 1)})
                    [["#", "Joueur", metrique_label]],
                    use_container_width=True, hide_index=True,
                )

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 2 — COMPARAISON JOUEURS
# ══════════════════════════════════════════════════════════════════════════════
with tab_comparaison:
    st.subheader("Comparaison joueurs")

    c1, c2 = st.columns([2, 2])
    with c1:
        source_label_c = st.selectbox("Source", list(SOURCES.keys()), key="cp_source")
        source_key_c   = SOURCES[source_label_c]
    metriques_c = get_metriques(source_key_c)
    with c2:
        metrique_label_c = st.selectbox("Métrique", list(metriques_c.keys()), key="cp_metrique")
        metrique_col_c   = metriques_c[metrique_label_c]

    df_src_c = get_df(source_key_c)
    df_src_c = slider_periode(df_src_c, key="cp_periode")
    joueurs_dispo = sorted(df_src_c["nom"].dropna().unique())
    joueurs_sel = st.multiselect(
        "Joueurs à comparer", joueurs_dispo,
        default=joueurs_dispo[:3] if len(joueurs_dispo) >= 3 else joueurs_dispo,
        max_selections=8, key="cp_joueurs",
    )

    vue = st.radio(
        "Vue", ["Évolution dans le temps", "Agrégé saison", "Radar (multi-métriques)"],
        horizontal=True, key="cp_vue",
    )

    if not joueurs_sel:
        st.info("Sélectionne au moins un joueur.")

    elif vue == "Radar (multi-métriques)":
        # Sélection des métriques pour le radar (indépendant de la métrique unique ci-dessus)
        metriques_dispo_radar = [
            m for m, c in metriques_c.items()
            if c in df_src_c.columns and c != "_count"
        ]
        defaut_radar = metriques_dispo_radar[:6]
        metriques_radar_sel = st.multiselect(
            "Métriques à afficher sur le radar",
            metriques_dispo_radar,
            default=defaut_radar,
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
            df_r     = df_src_c[df_src_c["nom"].isin(joueurs_sel)].copy()
            df_agg_r = df_r.groupby("nom")[cols_r].mean()

            # Normalisation sur le max de l'équipe entière (pas juste la sélection)
            df_equipe_agg = df_src_c.groupby("nom")[cols_r].mean()
            max_equipe = {col: df_equipe_agg[col].max() for col in cols_r}

            df_norm = df_agg_r.copy()
            for col in cols_r:
                mx = max_equipe[col]
                df_norm[col] = (df_agg_r[col] / mx * 100) if mx > 0 else 0

            # Calcul de la médiane de référence
            df_mediane = None
            label_mediane = ""
            if ref_mediane == "Médiane équipe":
                df_mediane = df_equipe_agg.median()
                label_mediane = "Médiane équipe"
            elif ref_mediane == "Médiane poste" and "poste" in df_src_c.columns:
                postes_sel = df_src_c[df_src_c["nom"].isin(joueurs_sel)]["poste"].dropna().unique()
                df_poste = df_src_c[df_src_c["poste"].isin(postes_sel)]
                df_mediane = df_poste.groupby("nom")[cols_r].mean().median()
                label_mediane = f"Médiane {' / '.join(postes_sel)}"

            # Palette Okabe-Ito — accessible daltoniens, lisible sur fond sombre
            RADAR_COLORS = [
                "#56B4E9",  # bleu ciel
                "#E69F00",  # orange
                "#009E73",  # vert teal
                "#CC79A7",  # rose/magenta
                "#F0E442",  # jaune
                "#D55E00",  # rouge orangé
                "#0072B2",  # bleu foncé
                "#7FDBFF",  # bleu cyan clair
            ]

            fig = go.Figure()
            theta = metriques_radar_sel + [metriques_radar_sel[0]]  # fermer le polygone

            # Trace médiane de référence (en premier pour qu'elle soit en dessous)
            if df_mediane is not None:
                vals_med = [df_mediane[c] / max_equipe[c] * 100 if max_equipe[c] > 0 else 0 for c in cols_r]
                vals_med_raw = [f"{df_mediane[c]:.1f}" for c in cols_r]
                fig.add_trace(go.Scatterpolar(
                    r=vals_med + [vals_med[0]],
                    theta=theta,
                    name=label_mediane,
                    text=vals_med_raw + [vals_med_raw[0]],
                    hovertemplate="%{theta}<br>%{text}<extra>" + label_mediane + "</extra>",
                    line=dict(color="#ffffff", width=2, dash="dash"),
                    fill="none",
                ))

            for i, joueur in enumerate(joueurs_sel):
                if joueur not in df_norm.index:
                    continue
                vals = [df_norm.loc[joueur, c] for c in cols_r]
                vals_raw = [df_agg_r.loc[joueur, c] for c in cols_r]
                vals_txt = [f"{v:.1f}" for v in vals_raw]
                color = RADAR_COLORS[i % len(RADAR_COLORS)]
                fig.add_trace(go.Scatterpolar(
                    r=vals + [vals[0]],
                    theta=theta,
                    name=joueur,
                    text=vals_txt + [vals_txt[0]],
                    hovertemplate="%{theta}<br>%{text}<extra>%{fullData.name}</extra>",
                    line=dict(color=color, width=2),
                    fill="toself",
                    fillcolor=color,
                    opacity=0.15,
                ))

            fig.update_layout(
                polar=dict(
                    bgcolor="#0d2240",
                    radialaxis=dict(
                        visible=True, range=[0, 100],
                        tickfont=dict(color="#666666", size=9),
                        gridcolor="#333333",
                    ),
                    angularaxis=dict(
                        tickfont=dict(color="#cccccc", size=11),
                        gridcolor="#333333",
                        linecolor="#444444",
                    ),
                ),
                paper_bgcolor="#0d2240",
                font_color="#c0d8f0",
                legend=dict(orientation="h", y=-0.1, font_color="#c0d8f0"),
                height=520,
                margin=dict(t=40, b=60, l=60, r=60),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Valeurs normalisées sur 100 (100 = meilleur de l'équipe pour cette métrique)")

    elif metrique_col_c not in df_src_c.columns:
        st.warning(f"Métrique `{metrique_col_c}` absente des données.")

    else:
        df_c = df_src_c[df_src_c["nom"].isin(joueurs_sel)].dropna(subset=[metrique_col_c, "date"])

        if df_c.empty:
            st.info("Aucune donnée pour cette sélection.")
        elif vue == "Évolution dans le temps":
            df_c["label_x"] = df_c["date"].dt.strftime("%d/%m")
            if "adversaire" in df_c.columns:
                df_c["label_x"] = df_c["label_x"] + " · " + df_c["adversaire"].fillna("?")
            elif "session_type" in df_c.columns:
                df_c["label_x"] = df_c["label_x"] + " · " + df_c["session_type"].fillna("?")

            fig = px.line(
                df_c.sort_values("date"),
                x="date", y=metrique_col_c,
                color="nom",
                markers=True,
                labels={"date": "", metrique_col_c: metrique_label_c, "nom": "Joueur"},
                color_discrete_sequence=["#56B4E9", "#E69F00", "#009E73", "#CC79A7",
                                          "#F0E442", "#D55E00", "#0072B2", "#7FDBFF"][:len(joueurs_sel)],
            )
            fig.update_layout(
                legend=dict(orientation="h", y=1.12, font_color="#c0d8f0"),
                height=420,
                **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "height"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            df_agg_c = (
                df_c.groupby("nom")[metrique_col_c]
                .mean()
                .reset_index()
                .rename(columns={"nom": "Joueur", metrique_col_c: metrique_label_c})
                .sort_values(metrique_label_c, ascending=False)
            )
            fig = px.bar(
                df_agg_c,
                x="Joueur", y=metrique_label_c,
                color="Joueur",
                color_discrete_sequence=["#56B4E9", "#E69F00", "#009E73", "#CC79A7",
                                          "#F0E442", "#D55E00", "#0072B2", "#7FDBFF"],
                labels={"Joueur": ""},
                text=metrique_label_c,
            )
            fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig.update_layout(showlegend=False, height=400,
                              **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "height"})
            st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 3 — TABLEAU LIBRE
# ══════════════════════════════════════════════════════════════════════════════
with tab_tableau:
    st.subheader("Tableau libre")

    c1, c2 = st.columns([2, 3])
    with c1:
        source_label_t = st.selectbox("Source", list(SOURCES.keys()), key="tb_source")
        source_key_t   = SOURCES[source_label_t]

    df_src_t = get_df(source_key_t)

    # Filtres
    with c2:
        joueurs_t = st.multiselect(
            "Joueur(s)", sorted(df_src_t["nom"].dropna().unique()),
            key="tb_joueurs", placeholder="Tous les joueurs",
        )

    if not df_src_t["date"].isna().all():
        d_min_t = df_src_t["date"].min().date()
        d_max_t = df_src_t["date"].max().date()
        d_from, d_to = st.slider(
            "Période",
            min_value=d_min_t, max_value=d_max_t,
            value=(d_min_t, d_max_t),
            format="DD/MM/YY",
            key="tb_periode",
        )
    else:
        d_from, d_to = None, None

    # Filtre type de séance (entraînements uniquement)
    if source_key_t == "entr" and "session_type" in df_src_t.columns:
        types_dispo = sorted(df_src_t["session_type"].dropna().unique())
        types_sel = st.multiselect("Type de séance", types_dispo, default=types_dispo, key="tb_type")
    else:
        types_sel = None

    # Application des filtres
    df_t = df_src_t.copy()
    if joueurs_t:
        df_t = df_t[df_t["nom"].isin(joueurs_t)]
    if d_from and d_to:
        df_t = df_t[(df_t["date"].dt.date >= d_from) & (df_t["date"].dt.date <= d_to)]
    if types_sel is not None:
        df_t = df_t[df_t["session_type"].isin(types_sel)]

    # Colonnes à afficher (exclut IDs internes et colonnes sources)
    cols_exclure = {"joueur_id", "match_id", "perf_match_id", "perf_entr_id",
                    "collision_id", "_source_file", "_source_file_gps", "_source_file_actions"}
    cols_afficher = [c for c in df_t.columns if c not in cols_exclure]

    st.caption(f"{len(df_t)} ligne(s) · {len(cols_afficher)} colonnes")

    # Sélection des colonnes à afficher
    metriques_t = get_metriques(source_key_t)
    cols_metriques = [c for c in metriques_t.values() if c in df_t.columns]
    cols_base = [c for c in ["nom", "poste", "date", "adversaire", "session_type", "match_titre"]
                 if c in df_t.columns]
    cols_defaut = cols_base + cols_metriques[:8]

    cols_sel = st.multiselect(
        "Colonnes à afficher", cols_afficher,
        default=[c for c in cols_defaut if c in cols_afficher],
        key="tb_cols",
    )

    if not cols_sel:
        st.info("Sélectionne au moins une colonne.")
    else:
        df_display = df_t[cols_sel].copy()
        if "date" in df_display.columns:
            df_display["date"] = df_display["date"].dt.strftime("%d/%m/%Y")

        st.dataframe(df_display, use_container_width=True, hide_index=True)

        # Téléchargement CSV
        csv = df_t[cols_sel].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️  Télécharger CSV",
            data=csv,
            file_name=f"rugby_data_{source_key_t}_export.csv",
            mime="text/csv",
        )
