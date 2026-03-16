"""
============================================================
DataSup Analytics — Gestion de la connexion à PostgreSQL
Supporte deux cibles : local et Supabase (variable ENV_CIBLE)
============================================================
"""

import os
import logging
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

load_dotenv()

journal = logging.getLogger(__name__)

# ============================================================
# Construction de l'URL de connexion selon la cible
# ============================================================

def _construire_url() -> str:
    """
    Construit l'URL de connexion SQLAlchemy à partir des
    variables d'environnement. La cible (local ou supabase)
    est déterminée par la variable ENV_CIBLE.

    Retourne
    --------
    str
        URL de connexion au format postgresql+psycopg2://...
    """
    cible = os.getenv("ENV_CIBLE", "local").lower()

    if cible == "supabase":
        hote      = os.getenv("SUPABASE_HOST")
        port      = os.getenv("SUPABASE_PORT", "5432")
        base      = os.getenv("SUPABASE_BASE", "postgres")
        utilisateur = os.getenv("SUPABASE_UTILISATEUR", "postgres")
        mdp       = os.getenv("SUPABASE_MOT_DE_PASSE")
    else:
        hote      = os.getenv("PG_LOCAL_HOST", "localhost")
        port      = os.getenv("PG_LOCAL_PORT", "5432")
        base      = os.getenv("PG_LOCAL_BASE", "datasup")
        utilisateur = os.getenv("PG_LOCAL_UTILISATEUR", "postgres")
        mdp       = os.getenv("PG_LOCAL_MOT_DE_PASSE")

    if not mdp:
        raise ValueError(
            f"Mot de passe PostgreSQL manquant pour la cible '{cible}'. "
            "Vérifier le fichier .env."
        )

    return (
        f"postgresql+psycopg2://{utilisateur}:{mdp}"
        f"@{hote}:{port}/{base}"
    )


# ============================================================
# Moteur SQLAlchemy (singleton)
# ============================================================

_moteur = None

def obtenir_moteur():
    """
    Retourne le moteur SQLAlchemy (singleton).
    Crée le moteur à la première invocation.
    """
    global _moteur
    if _moteur is None:
        url = _construire_url()
        _moteur = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,   # Vérifie la connexion avant utilisation
            echo=False,
        )
        journal.info(
            "Moteur SQLAlchemy initialisé (cible : %s)",
            os.getenv("ENV_CIBLE", "local")
        )
    return _moteur


def tester_connexion() -> bool:
    """
    Vérifie que la connexion à la base de données est opérationnelle.

    Retourne
    --------
    bool
        True si la connexion réussit, False sinon.
    """
    try:
        with obtenir_moteur().connect() as conn:
            resultat = conn.execute(text("SELECT version()")).scalar()
            journal.info("Connexion OK — %s", resultat)
            return True
    except OperationalError as e:
        journal.error("Connexion échouée : %s", e)
        return False


@contextmanager
def session_bdd():
    """
    Gestionnaire de contexte pour une session SQLAlchemy.
    Gère automatiquement le commit/rollback et la fermeture.

    Usage
    -----
    with session_bdd() as session:
        session.execute(text("SELECT 1"))
    """
    Session = sessionmaker(bind=obtenir_moteur())
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tester_connexion()
