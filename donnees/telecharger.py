"""
============================================================
DataSup Analytics — Téléchargement des datasets open data
Source : data.gouv.fr (API v1)
============================================================

Datasets ciblés :
  - Parcoursup 2021 à 2023 (vœux + admissions)
  - Insertion professionnelle des diplômés de Master (MESRI)
  - Insertion professionnelle des diplômés de Licence (MESRI)

Usage :
    python donnees/telecharger.py
"""

import os
import requests
import logging
from pathlib import Path

# --- Configuration du journal ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger(__name__)

# --- Dossier de destination ---
DOSSIER_BRUT = Path(__file__).parent / "brutes"
DOSSIER_BRUT.mkdir(parents=True, exist_ok=True)

# --- API data.gouv.fr ---
API_BASE = "https://www.data.gouv.fr/api/1/datasets"

# Identifiants des datasets sur data.gouv.fr
# Format : { "nom_fichier_local": ("slug_dataset", "mot_cle_filtre") }
# Le mot-clé filtre permet de choisir la bonne ressource dans un dataset
# qui contient plusieurs fichiers (ex : plusieurs années)
DATASETS = {
    "parcoursup_2023.csv": (
        "parcoursup-2023-voeux-de-poursuite-detudes-et-de-reorientation-dans-l"
        "enseignement-superieur-et-reponses-des-etablissements",
        "2023",
    ),
    "parcoursup_2022.csv": (
        "parcoursup-2022-voeux-de-poursuite-detudes-et-de-reorientation-dans-l"
        "enseignement-superieur-et-reponses-des-etablissements",
        "2022",
    ),
    "parcoursup_2021.csv": (
        "parcoursup-2021-voeux-de-poursuite-detudes-et-de-reorientation-dans-l"
        "enseignement-superieur-et-reponses-des-etablissements",
        "2021",
    ),
    "insertion_master.csv": (
        "insertion-professionnelle-des-diplomes-de-master-en-universite",
        "csv",
    ),
    "insertion_licence.csv": (
        "insertion-professionnelle-des-diplomes-de-licence-en-universite",
        "csv",
    ),
}


def recuperer_url_ressource(slug: str, mot_cle: str) -> str | None:
    """
    Interroge l'API data.gouv.fr pour récupérer l'URL directe de téléchargement
    d'un fichier CSV dans un dataset donné.

    Paramètres
    ----------
    slug : str
        Identifiant unique du dataset sur data.gouv.fr.
    mot_cle : str
        Sous-chaîne présente dans le titre ou l'URL de la ressource cible.

    Retourne
    --------
    str | None
        URL directe du fichier CSV, ou None si introuvable.
    """
    url_api = f"{API_BASE}/{slug}/"
    try:
        reponse = requests.get(url_api, timeout=15)
        reponse.raise_for_status()
    except requests.RequestException as e:
        journal.error("Erreur lors de l'interrogation de l'API : %s", e)
        return None

    donnees = reponse.json()
    ressources = donnees.get("resources", [])

    for ressource in ressources:
        titre = ressource.get("title", "").lower()
        url = ressource.get("url", "").lower()
        format_res = ressource.get("format", "").lower()

        if mot_cle.lower() in titre or mot_cle.lower() in url:
            if format_res in ("csv", "text/csv") or url.endswith(".csv"):
                return ressource["url"]

    # Si aucun filtre ne correspond, retourner le premier CSV disponible
    for ressource in ressources:
        format_res = ressource.get("format", "").lower()
        url = ressource.get("url", "")
        if format_res in ("csv", "text/csv") or url.lower().endswith(".csv"):
            journal.warning(
                "Mot-clé '%s' non trouvé, utilisation du premier CSV disponible.", mot_cle
            )
            return url

    return None


def telecharger_fichier(url: str, destination: Path) -> bool:
    """
    Télécharge un fichier depuis une URL vers un chemin local.

    Paramètres
    ----------
    url : str
        URL du fichier à télécharger.
    destination : Path
        Chemin local de destination.

    Retourne
    --------
    bool
        True si le téléchargement a réussi, False sinon.
    """
    if destination.exists():
        journal.info("Déjà présent, téléchargement ignoré : %s", destination.name)
        return True

    journal.info("Téléchargement : %s", destination.name)
    try:
        with requests.get(url, stream=True, timeout=60) as reponse:
            reponse.raise_for_status()
            taille_totale = int(reponse.headers.get("content-length", 0))
            taille_telechargee = 0

            with open(destination, "wb") as fichier:
                for bloc in reponse.iter_content(chunk_size=8192):
                    fichier.write(bloc)
                    taille_telechargee += len(bloc)

        taille_mo = taille_telechargee / (1024 * 1024)
        journal.info("Téléchargé : %s (%.1f Mo)", destination.name, taille_mo)
        return True

    except requests.RequestException as e:
        journal.error("Échec du téléchargement de %s : %s", destination.name, e)
        if destination.exists():
            destination.unlink()  # Supprimer le fichier partiel
        return False


def telecharger_tous() -> None:
    """
    Télécharge l'ensemble des datasets définis dans DATASETS.
    Affiche un résumé en fin d'exécution.
    """
    succes = 0
    echecs = []

    for nom_fichier, (slug, mot_cle) in DATASETS.items():
        destination = DOSSIER_BRUT / nom_fichier

        journal.info("--- Traitement : %s ---", nom_fichier)
        url = recuperer_url_ressource(slug, mot_cle)

        if url is None:
            journal.error("URL introuvable pour : %s", nom_fichier)
            echecs.append(nom_fichier)
            continue

        journal.info("URL trouvée : %s", url[:80] + "..." if len(url) > 80 else url)

        if telecharger_fichier(url, destination):
            succes += 1
        else:
            echecs.append(nom_fichier)

    # --- Résumé ---
    journal.info("=" * 50)
    journal.info("Téléchargements terminés : %d/%d réussis", succes, len(DATASETS))
    if echecs:
        journal.warning("Fichiers en échec : %s", ", ".join(echecs))
    journal.info("Fichiers disponibles dans : %s", DOSSIER_BRUT)


if __name__ == "__main__":
    telecharger_tous()
