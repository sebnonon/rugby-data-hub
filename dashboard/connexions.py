"""
Suivi des connexions au dashboard.
Logue une entrée dans la table `connexion` Supabase à chaque nouvelle session.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "pipeline"))
from supabase_client import get_client


def log_connexion(nom: str, email: str | None, session_id: str) -> None:
    """Insère une ligne dans la table connexion (silencieux en cas d'erreur)."""
    try:
        get_client().table("connexion").insert({
            "nom":        nom.strip(),
            "email":      email.strip() if email else None,
            "session_id": session_id,
        }).execute()
    except Exception:
        pass  # Ne pas bloquer l'accès si Supabase est indisponible
