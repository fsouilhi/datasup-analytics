"""
============================================================
DataSup Analytics — Gestion de la connexion à PostgreSQL
Supporte deux cibles : local et Supabase (variable ENV_CIBLE)
Utilise psycopg2.connect(**params) pour éviter les problèmes
avec les caractères spéciaux dans le mot de passe.
============================================================
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

journal = logging.getLogger(__name__)

_moteur = None


def _params_connexion() -> dict:
    cible = os.getenv("ENV_CIBLE", "local").lower()
    if cible == "supabase":
        mdp = os.getenv("SUPABASE_MOT_DE_PASSE")
        if not mdp:
            raise ValueError("SUPABASE_MOT_DE_PASSE manquant.")
        return {
            "host":     os.getenv("SUPABASE_HOST"),
            "port":     int(os.getenv("SUPABASE_PORT", "5432")),
            "dbname":   os.getenv("SUPABASE_BASE", "postgres"),
            "user":     os.getenv("SUPABASE_UTILISATEUR", "postgres"),
            "password": mdp,
        }
    else:
        mdp = os.getenv("PG_LOCAL_MOT_DE_PASSE")
        if not mdp:
            raise ValueError("PG_LOCAL_MOT_DE_PASSE manquant.")
        return {
            "host":     os.getenv("PG_LOCAL_HOST", "localhost"),
            "port":     int(os.getenv("PG_LOCAL_PORT", "5432")),
            "dbname":   os.getenv("PG_LOCAL_BASE", "datasup"),
            "user":     os.getenv("PG_LOCAL_UTILISATEUR", "postgres"),
            "password": mdp,
        }


def _construire_url() -> str:
    from urllib.parse import quote_plus
    p = _params_connexion()
    return (
        f"postgresql+psycopg2://{p['user']}:{quote_plus(p['password'])}"
        f"@{p['host']}:{p['port']}/{p['dbname']}"
    )


def obtenir_moteur():
    global _moteur
    if _moteur is None:
        params = _params_connexion()

        def creator():
            return psycopg2.connect(**params)

        _moteur = create_engine(
            "postgresql+psycopg2://",
            creator=creator,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
        )
        journal.info(
            "Moteur SQLAlchemy initialisé (cible : %s)",
            os.getenv("ENV_CIBLE", "local")
        )
    return _moteur


def tester_connexion() -> bool:
    try:
        with obtenir_moteur().connect() as conn:
            resultat = conn.execute(text("SELECT version()")).scalar()
            journal.info("Connexion OK — %s", resultat)
            return True
    except Exception as e:
        journal.error("Connexion échouée : %s", e)
        return False


@contextmanager
def session_bdd():
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
