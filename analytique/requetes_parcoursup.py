"""
============================================================
DataSup Analytics — Requêtes analytiques Parcoursup
============================================================
Chaque fonction retourne un DataFrame pandas prêt à visualiser.
Les requêtes utilisent fenêtrage, CTEs et agrégations avancées.
============================================================
"""
import logging
import pandas as pd
from sqlalchemy import text
from etl.connexion import obtenir_moteur

journal = logging.getLogger(__name__)


def classement_selectivite(annee: int = 2023, domaine: str = None,
                           limite: int = 20) -> pd.DataFrame:
    """
    Classement des formations par sélectivité (taux d'accès)
    avec rang calculé par fonction de fenêtrage RANK().

    Colonnes retournées :
        etablissement, domaine, taux_acces, rang_selectivite,
        nb_admis, nb_voeux, annee
    """
    filtre_domaine = "AND d.libelle = :domaine" if domaine else ""
    requete = f"""
        SELECT
            LEFT(e.nom, 60)  AS etablissement,
            e.academie,
            d.libelle        AS domaine,
            f.niveau,
            LEFT(f.libelle, 80) AS formation,
            p.taux_acces,
            p.nb_admis,
            p.nb_voeux,
            RANK() OVER (
                PARTITION BY d.id_domaine
                ORDER BY p.taux_acces ASC
            ) AS rang_selectivite,
            c.annee
        FROM parcoursup    p
        JOIN formation     f ON f.id_form     = p.id_form
        JOIN etablissement e ON e.id_etab     = f.id_etab
        JOIN domaine       d ON d.id_domaine  = f.id_domaine
        JOIN campagne      c ON c.id_campagne = p.id_campagne
        WHERE p.taux_acces IS NOT NULL
          AND p.taux_acces > 0
          AND p.nb_voeux   > 10
          AND f.libelle != 'Non renseigne'
          AND c.annee      = :annee
          {filtre_domaine}
        ORDER BY p.taux_acces ASC
        LIMIT :limite
    """
    params = {"annee": annee, "limite": limite}
    if domaine:
        params["domaine"] = domaine

    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn, params=params)


def evolution_admissions_par_domaine() -> pd.DataFrame:
    """
    Évolution du nombre total d'admis et du taux d'accès moyen
    par domaine et par année (2021–2023).

    Utilise une CTE + LAG() pour calculer la variation annuelle.

    Colonnes retournées :
        domaine, annee, total_admis, taux_acces_moyen,
        delta_admis, delta_taux
    """
    requete = """
        WITH agregat AS (
            SELECT
                d.libelle                  AS domaine,
                c.annee,
                SUM(p.nb_admis)            AS total_admis,
                AVG(p.taux_acces)          AS taux_acces_moyen,
                SUM(p.nb_voeux)            AS total_voeux
            FROM parcoursup    p
            JOIN formation     f ON f.id_form     = p.id_form
            JOIN domaine       d ON d.id_domaine  = f.id_domaine
            JOIN campagne      c ON c.id_campagne = p.id_campagne
            WHERE p.nb_admis IS NOT NULL
            GROUP BY d.libelle, c.annee
        )
        SELECT
            domaine,
            annee,
            total_admis,
            ROUND(taux_acces_moyen::numeric, 2)   AS taux_acces_moyen,
            total_voeux,
            total_admis - LAG(total_admis) OVER (
                PARTITION BY domaine ORDER BY annee
            )                                      AS delta_admis,
            ROUND((taux_acces_moyen - LAG(taux_acces_moyen) OVER (
                PARTITION BY domaine ORDER BY annee
            ))::numeric, 2)                        AS delta_taux
        FROM agregat
        ORDER BY domaine, annee
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn)


def top_formations_selectivite_par_academie(annee: int = 2023,
                                            top_n: int = 3) -> pd.DataFrame:
    """
    Top N formations les plus sélectives par académie.
    Utilise ROW_NUMBER() pour sélectionner les N premières par groupe.

    Colonnes retournées :
        academie, rang, formation, etablissement, taux_acces, nb_voeux
    """
    requete = """
        WITH classes AS (
            SELECT
                e.academie,
                LEFT(f.libelle, 70)  AS formation,
                LEFT(e.nom, 50)      AS etablissement,
                f.niveau,
                p.taux_acces,
                p.nb_voeux,
                ROW_NUMBER() OVER (
                    PARTITION BY e.academie
                    ORDER BY p.taux_acces ASC
                ) AS rang
            FROM parcoursup    p
            JOIN formation     f ON f.id_form     = p.id_form
            JOIN etablissement e ON e.id_etab     = f.id_etab
            JOIN campagne      c ON c.id_campagne = p.id_campagne
            WHERE p.taux_acces IS NOT NULL
              AND p.nb_voeux   > 50
              AND e.academie   IS NOT NULL
              AND c.annee      = :annee
        )
        SELECT academie, rang, formation, etablissement,
               taux_acces, nb_voeux, niveau
        FROM classes
        WHERE rang <= :top_n
        ORDER BY academie, rang
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn,
                           params={"annee": annee, "top_n": top_n})


def profil_admis_par_niveau(annee: int = 2023) -> pd.DataFrame:
    """
    Profil moyen des admis (boursiers, mention TB, femmes)
    agrégé par niveau de formation.

    Colonnes retournées :
        niveau, nb_formations, moy_taux_acces, moy_pct_femmes,
        moy_pct_boursiers, moy_pct_mention_tb, total_admis
    """
    requete = """
        SELECT
            f.niveau,
            COUNT(*)                              AS nb_formations,
            ROUND(AVG(p.taux_acces)::numeric, 1)  AS moy_taux_acces,
            ROUND(AVG(p.pct_femmes)::numeric, 1)  AS moy_pct_femmes,
            ROUND(AVG(p.pct_boursiers)::numeric, 1) AS moy_pct_boursiers,
            ROUND(AVG(p.pct_mention_tb)::numeric, 1) AS moy_pct_mention_tb,
            SUM(p.nb_admis)                       AS total_admis
        FROM parcoursup    p
        JOIN formation     f ON f.id_form     = p.id_form
        JOIN campagne      c ON c.id_campagne = p.id_campagne
        WHERE c.annee = :annee
        GROUP BY f.niveau
        ORDER BY moy_taux_acces ASC
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn, params={"annee": annee})


def stats_globales(annee: int = 2023) -> dict:
    """
    Indicateurs clés pour les cartes de synthèse du dashboard.

    Retourne un dict avec :
        nb_formations, nb_etablissements, total_voeux,
        total_admis, taux_acces_moyen
    """
    requete = """
        SELECT
            COUNT(DISTINCT p.id_form)          AS nb_formations,
            COUNT(DISTINCT e.id_etab)          AS nb_etablissements,
            SUM(p.nb_voeux)                    AS total_voeux,
            SUM(p.nb_admis)                    AS total_admis,
            ROUND(AVG(p.taux_acces)::numeric, 1) AS taux_acces_moyen
        FROM parcoursup    p
        JOIN formation     f ON f.id_form     = p.id_form
        JOIN etablissement e ON e.id_etab     = f.id_etab
        JOIN campagne      c ON c.id_campagne = p.id_campagne
        WHERE c.annee = :annee
    """
    with obtenir_moteur().connect() as conn:
        row = conn.execute(text(requete), {"annee": annee}).fetchone()
        return {
            "nb_formations": int(row.nb_formations or 0),
            "nb_etablissements": int(row.nb_etablissements or 0),
            "total_voeux": int(row.total_voeux or 0),
            "total_admis": int(row.total_admis or 0),
            "taux_acces_moyen": float(row.taux_acces_moyen or 0),
        }
