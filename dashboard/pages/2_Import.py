"""
Page Import / Export — Rugby Data Hub
Import : upload CSV STATSports → Supabase.
Export : tableau libre filtré avec téléchargement CSV.
"""

import sys
from pathlib import Path
import streamlit as st
from PIL import Image
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent / "pipeline"))
from parsers import PARSERS
from supabase_client import get_client

st.set_page_config(
    page_title="Rugby Data Hub — Import / Export",
    page_icon=Image.open(Path(__file__).parent.parent / "logo.jpg") if (Path(__file__).parent.parent / "logo.jpg").exists() else "🏉",
    layout="wide",
)

st.markdown("""
<style>
    .stApp { background-color: #f0f6fb; color: #071626; }
    [data-testid="stSidebar"] { background-color: #ffffff; }
    h1, h2, h3 { color: #1a3a5c; }
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
    [data-testid="stTextInput"] input { background-color: #ffffff; color: #071626; }
    [data-testid="stSelectbox"] label,
    [data-testid="stMultiSelect"] label { color: #2a6080; }
    hr { border-color: #c0d8ea; }
    .stAlert { border-radius: 8px; }
    [data-testid="stDataFrame"] { border: 1px solid #c0d8ea; border-radius: 8px; }
    .block-container { padding-top: 1.5rem !important; }
</style>
""", unsafe_allow_html=True)

col_logo, col_titre = st.columns([1, 8], gap="small")
with col_logo:
    logo_path = Path(__file__).parent.parent / "logo.jpg"
    if logo_path.exists():
        st.markdown('<div style="margin-top: 20px"></div>', unsafe_allow_html=True)
        st.image(str(logo_path), width=80)
with col_titre:
    st.markdown('<div style="margin-top: 22px"></div>', unsafe_allow_html=True)
    st.markdown("## Import / Export")

st.divider()

tab_import, tab_export = st.tabs(["📤 Import", "⬇️ Export"])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET IMPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_import:
    LABELS = {
        "gps_match":        "GPS Match",
        "gps_entrainement": "GPS Entraînement",
        "gps_collision":    "GPS Collision",
        "gps_melee":        "GPS Mêlée",
        "actions_match":    "Actions Match (codage vidéo)",
    }

    NOMENCLATURE_AIDE = {
        "gps_match":        "gps_match_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_match_REC-OMR_2025-11-15.csv",
        "gps_entrainement": "gps_entrainement_YYYY-MM-DD_(Reprise|J-2|J-1|J+2).csv\nEx : gps_entrainement_2025-11-14_J-2.csv",
        "gps_collision":    "gps_collision_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_collision_REC-NRC_2025-09-13.csv",
        "gps_melee":        "gps_melee_EQ1-EQ2_YYYY-MM-DD.csv\nEx : gps_melee_REC-CSBJ_2025-11-08.csv",
        "actions_match":    "actions_match_EQ1-EQ2_YYYY-MM-DD.csv\nEx : actions_match_REC-OMR_2025-11-15.csv",
    }

    TABLES_CIBLES = {
        "gps_match":        ["perf_match (colonnes GPS)"],
        "gps_entrainement": ["perf_entrainement"],
        "gps_collision":    ["collision"],
        "gps_melee":        ["melee"],
        "actions_match":    ["perf_match (colonnes actions)", "touche", "match (métriques collectives)"],
    }

    col_sel, col_info = st.columns([2, 3])

    with col_sel:
        type_fichier = st.selectbox(
            "Type de fichier",
            list(PARSERS.keys()),
            format_func=lambda k: LABELS[k],
        )

    with col_info:
        st.markdown("**Nomenclature attendue :**")
        st.code(NOMENCLATURE_AIDE[type_fichier], language=None)
        st.markdown(f"**Tables alimentées :** {', '.join(TABLES_CIBLES[type_fichier])}")

    st.divider()

    uploaded = st.file_uploader("Choisir un fichier CSV", type=["csv"])

    if uploaded is not None:
        # Validation du nom de fichier
        parser = PARSERS[type_fichier]
        valide, erreur, metadata = parser.validate_filename(uploaded.name)

        if not valide:
            st.error(f"**Nom de fichier invalide**\n\n{erreur}")
        else:
            st.success(f"Nomenclature valide : **{uploaded.name}**")
            if metadata:
                meta_str = " · ".join(f"{k} = `{v}`" for k, v in metadata.items())
                st.caption(f"Métadonnées extraites : {meta_str}")

            # Chargement et validation du contenu
            try:
                df_raw = pd.read_csv(uploaded)
            except Exception as e:
                st.error(f"Impossible de lire le CSV : {e}")
                df_raw = None

            if df_raw is not None:
                cols_manquantes = [c for c in parser.REQUIRED_COLUMNS if c not in df_raw.columns]
                for attr, label in [("PLAYER_NAME_COLS", "colonne joueur"), ("DISTANCE_COLS", "colonne distance")]:
                    alt_cols = getattr(parser, attr, None)
                    if alt_cols and not any(c in df_raw.columns for c in alt_cols):
                        cols_manquantes.append(f"{label} ({' ou '.join(alt_cols)})")
                if cols_manquantes:
                    st.error(f"Colonnes requises manquantes : {', '.join(cols_manquantes)}")
                else:
                    st.markdown(f"**{len(df_raw)} lignes brutes** détectées dans le fichier.")

                    @st.cache_data(ttl=120)
                    def load_refs():
                        c = get_client()
                        joueurs = pd.DataFrame(c.table("joueur").select("joueur_id, nom").execute().data)
                        matchs  = pd.DataFrame(c.table("match").select("match_id, session_title").execute().data)
                        return joueurs, matchs

                    with st.spinner("Chargement des références…"):
                        joueurs_df, matchs_df = load_refs()

                    with st.spinner("Parsing du fichier…"):
                        try:
                            if type_fichier == "actions_match":
                                parsed = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
                                df_preview = parsed["perf_match_actions"]
                            else:
                                df_preview = parser.parse(df_raw, joueurs_df, matchs_df, filename=uploaded.name)
                            parse_ok = True
                        except Exception as e:
                            st.error(f"Erreur lors du parsing : {e}")
                            parse_ok = False

                    if parse_ok:
                        col1, col2, col3 = st.columns(3)
                        lignes_total = len(df_preview)
                        sans_joueur  = df_preview["joueur_id"].isna().sum() if "joueur_id" in df_preview.columns else 0
                        sans_match   = df_preview["match_id"].isna().sum()  if "match_id"  in df_preview.columns else 0

                        col1.metric("Lignes parsées",    lignes_total)
                        col2.metric("Sans joueur_id ⚠️", sans_joueur)
                        col3.metric("Sans match_id ⚠️",  sans_match)

                        with st.expander("Aperçu des données parsées (5 premières lignes)"):
                            st.dataframe(df_preview.head(), use_container_width=True, hide_index=True)

                        st.divider()

                        if st.button("Importer dans Supabase", type="primary"):
                            st.info("Ceci est une démo — l'import n'est pas réellement disponible.")


# ══════════════════════════════════════════════════════════════════════════════
# ONGLET EXPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_export:
    METRIQUES_MATCH_EXP = {
        "Distance totale (m)":       "total_distance",
        "Vitesse max (km/h)":        "max_speed",
        "Sprints":                   "sprints",
        "Distance sprint (m)":       "sprint_distance",
        "HSR (m)":                   "high_speed_running_absolute",
        "Distance HML (m)":          "hml_distance",
        "Accélérations":             "accelerations_absolute",
        "Décélérations":             "decelerations_absolute",
        "Charge de collision":       "collision_load",
        "Nb collisions GPS":         "collisions",
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

    METRIQUES_ENTR_EXP = {
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

    METRIQUES_COL_EXP = {
        "Nb collisions":             "_count",
        "Charge totale":             "collision_load_sum",
        "Charge moy. / collision":   "collision_load_mean",
        "Time to feet moy. (s)":     "time_to_feet_mean",
        "Post-collision accél. moy.": "post_collision_accel_mean",
    }

    SOURCES_EXP = {
        "Matchs (GPS + actions)": "match",
        "Entraînements":          "entr",
        "Collisions":             "col",
    }

    @st.cache_data(ttl=300)
    def load_match_exp():
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
    def load_entr_exp():
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

    @st.cache_data(ttl=300)
    def load_col_exp():
        c = get_client()
        res = (
            c.table("collision")
            .select("joueur_id, match_id, collision_load, time_to_feet, post_collision_accel, "
                    "joueur(nom, prenom, poste_principal), match(date, adversaire)")
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
                "prenom":               r["joueur"]["prenom"] if r["joueur"] else None,
                "poste":                r["joueur"]["poste_principal"] if r["joueur"] else None,
                "date":                 r["match"]["date"] if r["match"] else None,
                "adversaire":           r["match"]["adversaire"] if r["match"] else None,
            })
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        agg = (
            df.groupby(["joueur_id", "match_id", "nom", "prenom", "poste", "date", "adversaire"])
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

    # ── Interface export ──────────────────────────────────────────────────────

    c1, c2 = st.columns([2, 3])
    with c1:
        source_label_e = st.selectbox("Source", list(SOURCES_EXP.keys()), key="ex_source")
    source_key_e = SOURCES_EXP[source_label_e]

    if source_key_e == "match":
        df_src_e = load_match_exp()
        metriques_e = METRIQUES_MATCH_EXP
    elif source_key_e == "entr":
        df_src_e = load_entr_exp()
        metriques_e = METRIQUES_ENTR_EXP
    else:
        df_src_e = load_col_exp()
        metriques_e = METRIQUES_COL_EXP

    with c2:
        # Labels "Prénom NOM"
        if "prenom" in df_src_e.columns:
            df_j_e = df_src_e[["nom", "prenom"]].drop_duplicates().dropna(subset=["nom"]).sort_values("nom")
            df_j_e["label"] = df_j_e.apply(
                lambda r: f"{r['prenom']} {r['nom']}" if pd.notna(r.get("prenom")) else r["nom"], axis=1
            )
            joueurs_dispo_e = df_j_e["label"].tolist()
            label_to_nom_e  = dict(zip(df_j_e["label"], df_j_e["nom"]))
        else:
            joueurs_dispo_e = sorted(df_src_e["nom"].dropna().unique())
            label_to_nom_e  = {j: j for j in joueurs_dispo_e}

        joueurs_labels_e = st.multiselect(
            "Joueur(s)", joueurs_dispo_e,
            key="ex_joueurs", placeholder="Tous les joueurs",
        )
    joueurs_sel_e = [label_to_nom_e[l] for l in joueurs_labels_e]

    # Slider période
    if "date" in df_src_e.columns and not df_src_e["date"].isna().all():
        d_min_e = df_src_e["date"].min().date()
        d_max_e = df_src_e["date"].max().date()
        d_from_e, d_to_e = st.slider(
            "Période",
            min_value=d_min_e, max_value=d_max_e,
            value=(d_min_e, d_max_e),
            format="DD/MM/YY",
            key="ex_periode",
        )
    else:
        d_from_e, d_to_e = None, None

    # Filtre type de séance (entraînements)
    if source_key_e == "entr" and "session_type" in df_src_e.columns:
        types_dispo_e = sorted(df_src_e["session_type"].dropna().unique())
        types_sel_e = st.multiselect("Type de séance", types_dispo_e, default=types_dispo_e, key="ex_type")
    else:
        types_sel_e = None

    # Application des filtres
    df_e = df_src_e.copy()
    if joueurs_sel_e:
        df_e = df_e[df_e["nom"].isin(joueurs_sel_e)]
    if d_from_e and d_to_e:
        df_e = df_e[(df_e["date"].dt.date >= d_from_e) & (df_e["date"].dt.date <= d_to_e)]
    if types_sel_e is not None:
        df_e = df_e[df_e["session_type"].isin(types_sel_e)]

    # Sélection des colonnes
    cols_exclure_e = {"joueur_id", "match_id", "perf_match_id", "perf_entr_id",
                      "collision_id", "_source_file", "_source_file_gps", "_source_file_actions"}
    cols_afficher_e = [c for c in df_e.columns if c not in cols_exclure_e]

    cols_metriques_e = [c for c in metriques_e.values() if c in df_e.columns]
    cols_base_e = [c for c in ["nom", "prenom", "poste", "date", "adversaire", "session_type", "match_titre"]
                   if c in df_e.columns]
    cols_defaut_e = cols_base_e + cols_metriques_e[:8]

    cols_sel_e = st.multiselect(
        "Colonnes à afficher", cols_afficher_e,
        default=[c for c in cols_defaut_e if c in cols_afficher_e],
        key="ex_cols",
    )

    st.caption(f"{len(df_e)} ligne(s) · {len(cols_sel_e)} colonnes sélectionnées")

    if not cols_sel_e:
        st.info("Sélectionne au moins une colonne.")
    else:
        df_display_e = df_e[cols_sel_e].copy()
        if "date" in df_display_e.columns:
            df_display_e["date"] = df_display_e["date"].dt.strftime("%d/%m/%Y")

        st.dataframe(df_display_e, use_container_width=True, hide_index=True)

        csv_e = df_e[cols_sel_e].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️  Télécharger CSV",
            data=csv_e,
            file_name=f"rugby_data_{source_key_e}_export.csv",
            mime="text/csv",
        )
