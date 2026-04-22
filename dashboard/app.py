"""
Point d'entrée du dashboard Rugby Data Hub — multi-page.
"""

import streamlit as st

st.set_page_config(page_title="Rugby Data Hub", page_icon="🏉", layout="wide")

pg = st.navigation([
    st.Page("pages/1_Performances.py",  title="Performances",   icon="📊"),
    st.Page("pages/3_Entrainements.py", title="Entraînements",  icon="🏋️"),
    st.Page("pages/4_Collisions.py",    title="Collisions",     icon="💥"),
    st.Page("pages/5_Melees.py",        title="Mêlées",         icon="🏉"),
    st.Page("pages/6_Explorer.py",      title="Explorateur",    icon="🔍"),
    st.Page("pages/2_Import.py",        title="Import données", icon="📤"),
])
pg.run()
