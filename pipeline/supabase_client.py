"""
Client Supabase pour le pipeline Rugby Data Hub.
Priorité : Streamlit Secrets (déploiement) → fichier .env (dev local).
"""

import os
from pathlib import Path
from supabase import create_client, Client


def _load_credentials() -> None:
    """Charge les credentials Supabase selon l'environnement d'exécution."""
    # 1. Streamlit Secrets (Streamlit Cloud)
    try:
        import streamlit as st
        if "SUPABASE_URL" in st.secrets:
            os.environ.setdefault("SUPABASE_URL", st.secrets["SUPABASE_URL"])
            os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", st.secrets["SUPABASE_SERVICE_ROLE_KEY"])
            return
    except Exception:
        pass

    # 2. Fichier .env (dev local)
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def get_client() -> Client:
    """Retourne un client Supabase authentifié."""
    _load_credentials()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise EnvironmentError(
            "Variables manquantes : SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY "
            "doivent être définies dans .env ou dans les Streamlit Secrets."
        )

    return create_client(url, key)
