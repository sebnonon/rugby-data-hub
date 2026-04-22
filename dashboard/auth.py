import streamlit as st


def require_auth() -> None:
    """Bloque l'accès à la page si l'utilisateur n'est pas identifié."""
    if "utilisateur" not in st.session_state:
        st.error("Accès refusé. Veuillez vous identifier sur la page d'accueil.")
        st.page_link("app.py", label="Retour à l'accueil", icon="🏠")
        st.stop()
