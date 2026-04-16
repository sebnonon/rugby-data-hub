"""
Point d'entrée du dashboard Rugby Data Hub — multi-page.
"""

import uuid
import streamlit as st
from connexions import log_connexion

# ── Identification obligatoire à chaque nouvelle session ─────────────────────
if "utilisateur" not in st.session_state:
    st.set_page_config(page_title="Rugby Data Hub", page_icon="🏉", layout="centered")
    st.markdown("""
    <style>
        .stApp { background-color: #0e0e0e; color: #f0f0f0; }
        h1 { color: #ffffff; font-weight: 800; }
        p  { color: #aaaaaa; }
        [data-testid="stForm"] { background-color: #1a1a1a; border: 1px solid #333;
                                  border-radius: 12px; padding: 24px; }
    </style>
    """, unsafe_allow_html=True)

    st.title("🏉 Rugby Data Hub")
    st.markdown("Identifie-toi pour accéder au dashboard.")

    with st.form("identification"):
        nom   = st.text_input("Nom *", placeholder="ex : Jean Dupont")
        email = st.text_input("Email (optionnel)", placeholder="ex : jean@club.fr")
        ok    = st.form_submit_button("Accéder au dashboard", use_container_width=True)

    if ok:
        if not nom.strip():
            st.error("Le nom est obligatoire.")
            st.stop()
        session_id = str(uuid.uuid4())
        log_connexion(nom, email, session_id)
        st.session_state["utilisateur"] = {"nom": nom.strip(), "email": email.strip() or None}
        st.session_state["session_id"]  = session_id
        st.rerun()
    else:
        st.stop()

# ── Navigation principale ─────────────────────────────────────────────────────
pg = st.navigation([
    st.Page("pages/1_Performances.py",  title="Performances",   icon="📊"),
    st.Page("pages/3_Entrainements.py", title="Entraînements",  icon="🏋️"),
    st.Page("pages/4_Collisions.py",    title="Collisions",     icon="💥"),
    st.Page("pages/5_Melees.py",        title="Mêlées",         icon="🏉"),
    st.Page("pages/6_Explorer.py",      title="Explorateur",    icon="🔍"),
    st.Page("pages/2_Import.py",        title="Import données", icon="📤"),
])
pg.run()
