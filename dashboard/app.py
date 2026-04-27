"""
Point d'entrée du dashboard Rugby Data Hub — multi-page.
"""

import streamlit as st

st.set_page_config(page_title="Rugby Data Hub", page_icon="🏉", layout="wide")

pg = st.navigation([
    st.Page("pages/1_Performances.py",  title="Perf. joueur",       icon="📊"),
    st.Page("pages/3_Entrainements.py", title="Suivi entraînement",  icon="🏋️"),
    st.Page("pages/7_Equipe.py",        title="Perf. équipe",        icon="🏉"),
    st.Page("pages/2_Import.py",        title="Import / Export",     icon="📤"),
])
pg.run()
