"""
============================================================
DataSup Analytics — Exploration des datasets bruts
À exécuter après telecharger.py
============================================================

Ce script lit chaque CSV téléchargé et produit un rapport
d'exploration : colonnes, types, valeurs manquantes, aperçu.
Ces informations servent à valider et affiner le schéma SQL.

Usage :
    python donnees/explorer.py
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

# --- Journal ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger(__name__)

DOSSIER_BRUT = Path(__file__).parent / "brutes"

# Colonnes d'intérêt connues dans Parcoursup
COLONNES_PARCOURSUP = [
    "cod_uai",            # Code UAI de l'établissement
    "g_ea_lib_vx",        # Libellé établissement
    "dep_lib",            # Département
    "region_etab_aff",    # Région
    "acad_mies",          # Académie
    "fili",               # Filière grande catégorie
    "lib_for_voe_ins",    # Libellé formation
    "capa_fin",           # Capacité d'accueil
    "nb_voe_pp",          # Nombre de vœux en phase principale
    "acc_tot",            # Nombre total d'admis
    "taux_acces",         # Taux d'accès
    "pct_din_fem",        # Part de femmes admises
    "pct_din_bour",       # Part de boursiers admis
    "pct_din_pro",        # Part de mentions TB parmi admis (variable selon année)
]

# Colonnes d'intérêt connues dans insertion pro
COLONNES_INSERTION = [
    "diplome",
    "domaine",
    "annee",
    "situation",
    "nombre_de_reponses",
    "taux_dinsertion",
    "emplois_cadre",
    "emplois_stables",
    "emplois_a_temps_plein",
    "salaire_net_median_des_emplois_a_temps_plein",
    "salaire_brut_annuel_estime",
]


def separateur(titre: str) -> None:
    """Affiche un séparateur visuel avec un titre."""
    largeur = 60
    print("\n" + "=" * largeur)
    print(f"  {titre}")
    print("=" * largeur)


def explorer_fichier(chemin: Path, colonnes_cibles: list[str] | None = None) -> pd.DataFrame | None:
    """
    Charge et explore un fichier CSV.

    Paramètres
    ----------
    chemin : Path
        Chemin vers le fichier CSV.
    colonnes_cibles : list[str] | None
        Liste de colonnes attendues à vérifier (optionnel).

    Retourne
    --------
    pd.DataFrame | None
        Le DataFrame chargé, ou None en cas d'erreur.
    """
    if not chemin.exists():
        journal.warning("Fichier absent : %s — lancer telecharger.py d'abord.", chemin.name)
        return None

    separateur(f"Exploration : {chemin.name}")

    # Essais successifs d'encodage (les CSV MESRI sont parfois en latin-1)
    for encodage in ("utf-8", "latin-1", "utf-8-sig"):
        try:
            df = pd.read_csv(chemin, sep=";", encoding=encodage, low_memory=False)
            journal.info("Encodage retenu : %s", encodage)
            break
        except UnicodeDecodeError:
            continue
    else:
        journal.error("Impossible de décoder le fichier : %s", chemin.name)
        return None

    # --- Informations générales ---
    print(f"\nDimensions         : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    print(f"Mémoire            : {df.memory_usage(deep=True).sum() / 1024**2:.1f} Mo")

    # --- Colonnes et types ---
    print("\n--- Colonnes et types ---")
    info_colonnes = pd.DataFrame({
        "colonne": df.columns,
        "type": df.dtypes.values,
        "manquants": df.isnull().sum().values,
        "pct_manquants": (df.isnull().mean().values * 100).round(1),
        "exemple": [str(df[c].dropna().iloc[0]) if not df[c].dropna().empty else "N/A"
                    for c in df.columns],
    })
    print(info_colonnes.to_string(index=False))

    # --- Vérification des colonnes cibles ---
    if colonnes_cibles:
        print("\n--- Vérification des colonnes cibles ---")
        trouvees = [c for c in colonnes_cibles if c in df.columns]
        manquantes = [c for c in colonnes_cibles if c not in df.columns]

        print(f"Trouvées ({len(trouvees)}/{len(colonnes_cibles)}) : {trouvees}")
        if manquantes:
            print(f"Absentes : {manquantes}")
            # Chercher des colonnes similaires
            for col_manquante in manquantes:
                similaires = [c for c in df.columns if col_manquante[:4].lower() in c.lower()]
                if similaires:
                    print(f"  → Colonnes similaires pour '{col_manquante}' : {similaires}")

    # --- Statistiques numériques ---
    colonnes_num = df.select_dtypes(include=[np.number]).columns.tolist()
    if colonnes_num:
        print(f"\n--- Statistiques numériques ({len(colonnes_num)} colonnes) ---")
        print(df[colonnes_num].describe().round(2).to_string())

    # --- Aperçu ---
    print("\n--- 3 premières lignes ---")
    print(df.head(3).to_string())

    return df


def verifier_jointures_possibles(
    df_parcoursup: pd.DataFrame | None,
    df_insertion: pd.DataFrame | None
) -> None:
    """
    Vérifie si des colonnes de jointure existent entre Parcoursup et insertion pro.
    """
    if df_parcoursup is None or df_insertion is None:
        return

    separateur("Analyse des jointures possibles")

    # Chercher des colonnes communes (par nom exact ou similaire)
    cols_ps = set(c.lower() for c in df_parcoursup.columns)
    cols_ins = set(c.lower() for c in df_insertion.columns)
    communes = cols_ps & cols_ins

    print(f"\nColonnes en commun (exact) : {communes if communes else 'Aucune'}")

    # Colonnes candidats pour la jointure domaine/filière
    cles_domaine_ps = [c for c in df_parcoursup.columns if any(
        k in c.lower() for k in ("fili", "domaine", "lib_for")
    )]
    cles_domaine_ins = [c for c in df_insertion.columns if any(
        k in c.lower() for k in ("domaine", "diplome", "disc")
    )]

    print(f"\nCandidats de jointure côté Parcoursup : {cles_domaine_ps}")
    print(f"Candidats de jointure côté Insertion   : {cles_domaine_ins}")


def main() -> None:
    """Point d'entrée principal de l'exploration."""

    fichiers = {
        "parcoursup_2023.csv": COLONNES_PARCOURSUP,
        "parcoursup_2022.csv": COLONNES_PARCOURSUP,
        "insertion_master.csv": COLONNES_INSERTION,
        "insertion_licence.csv": COLONNES_INSERTION,
    }

    dataframes = {}
    for nom, colonnes_cibles in fichiers.items():
        df = explorer_fichier(DOSSIER_BRUT / nom, colonnes_cibles)
        if df is not None:
            dataframes[nom] = df

    # Analyse des jointures entre les deux sources
    df_ps = dataframes.get("parcoursup_2023.csv")
    df_ins = dataframes.get("insertion_master.csv")
    verifier_jointures_possibles(df_ps, df_ins)

    separateur("Exploration terminée")
    print(f"Datasets chargés : {list(dataframes.keys())}")
    print("Prochaine étape : valider le schéma SQL dans bdd/schema.sql")


if __name__ == "__main__":
    main()
