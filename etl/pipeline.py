"""
============================================================
DataSup Analytics — Pipeline ETL orchestrateur
============================================================
Lance le chargement complet dans l'ordre correct :
  1. Vérification de la connexion
  2. Parcoursup 2021, 2022, 2023
  3. Insertion pro Master
  4. Insertion pro Licence Pro
  5. Rapport de volumétrie finale
============================================================

Usage :
    python -m etl.pipeline
    python -m etl.pipeline --etape parcoursup
    python -m etl.pipeline --etape insertion
"""

import argparse
import logging
import sys
import time

from sqlalchemy import text

from etl.connexion import tester_connexion, obtenir_moteur
from etl.charger_parcoursup import charger_parcoursup
from etl.charger_insertion import charger_insertion

# --- Configuration du journal ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger(__name__)


def rapport_volumetrie() -> None:
    """Affiche le nombre de lignes dans chaque table après chargement."""
    tables = ["etablissement", "formation", "campagne",
              "domaine", "parcoursup", "insertion_pro"]

    journal.info("=" * 50)
    journal.info("Rapport de volumétrie finale")
    journal.info("=" * 50)

    moteur = obtenir_moteur()
    with moteur.connect() as conn:
        for table in tables:
            try:
                n = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar()
                journal.info("  %-20s : %8d lignes", table, n)
            except Exception as e:
                journal.warning("  %-20s : erreur — %s", table, e)


def etape_parcoursup() -> None:
    """Charge les trois années Parcoursup."""
    for annee in (2021, 2022, 2023):
        debut = time.time()
        charger_parcoursup(annee)
        duree = time.time() - debut
        journal.info("Parcoursup %d terminé en %.1f s", annee, duree)


def etape_insertion() -> None:
    """Charge l'insertion pro Master et Licence Pro."""
    for type_diplome in ("master", "licence"):
        debut = time.time()
        charger_insertion(type_diplome)
        duree = time.time() - debut
        journal.info("Insertion %s terminée en %.1f s", type_diplome, duree)


def pipeline_complet() -> None:
    """Lance le pipeline ETL complet."""
    debut_total = time.time()

    journal.info("DataSup Analytics — Démarrage du pipeline ETL")
    journal.info("=" * 50)

    # Vérification de la connexion
    if not tester_connexion():
        journal.error("Connexion impossible — vérifier le fichier .env")
        sys.exit(1)

    # Chargement Parcoursup
    etape_parcoursup()

    # Chargement insertion pro
    etape_insertion()

    # Rapport final
    rapport_volumetrie()

    duree_totale = time.time() - debut_total
    journal.info("=" * 50)
    journal.info("Pipeline terminé en %.1f s (%.1f min)",
                 duree_totale, duree_totale / 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL DataSup Analytics"
    )
    parser.add_argument(
        "--etape",
        choices=["tout", "parcoursup", "insertion", "rapport"],
        default="tout",
        help="Étape à exécuter (défaut : tout)"
    )
    args = parser.parse_args()

    if args.etape == "tout":
        pipeline_complet()
    elif args.etape == "parcoursup":
        if not tester_connexion():
            sys.exit(1)
        etape_parcoursup()
        rapport_volumetrie()
    elif args.etape == "insertion":
        if not tester_connexion():
            sys.exit(1)
        etape_insertion()
        rapport_volumetrie()
    elif args.etape == "rapport":
        rapport_volumetrie()


if __name__ == "__main__":
    main()
