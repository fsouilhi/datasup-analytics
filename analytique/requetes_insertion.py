"""
============================================================
DataSup Analytics — Requêtes analytiques Insertion pro
============================================================
"""
import pandas as pd
from sqlalchemy import text
from etl.connexion import obtenir_moteur


def salaires_par_domaine() -> pd.DataFrame:
    """
    Salaire net médian et taux d'emploi par domaine de formation.
    Filtre sur les données avec au moins 30 répondants.

    Colonnes : domaine, nb_formations, salaire_median_moyen,
               taux_emploi_moyen, pct_cadre_moyen
    """
    requete = """
        SELECT
            d.libelle                                   AS domaine,
            d.secteur,
            COUNT(*)                                    AS nb_formations,
            ROUND(AVG(i.salaire_median)::numeric, 0)    AS salaire_median_moyen,
            ROUND(AVG(i.taux_emploi_18m)::numeric, 1)   AS taux_emploi_moyen,
            ROUND(AVG(i.pct_emploi_cadre)::numeric, 1)  AS pct_cadre_moyen,
            ROUND(AVG(i.pct_temps_plein)::numeric, 1)   AS pct_temps_plein_moyen
        FROM insertion_pro  i
        JOIN formation      f ON f.id_form    = i.id_form
        JOIN domaine        d ON d.id_domaine = f.id_domaine
        WHERE i.nb_repondants >= 30
          AND i.salaire_median  IS NOT NULL
        GROUP BY d.libelle, d.secteur
        ORDER BY salaire_median_moyen DESC NULLS LAST
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn)


def evolution_insertion_par_domaine() -> pd.DataFrame:
    """
    Évolution temporelle du taux d'emploi et du salaire médian
    par domaine, avec delta annuel via LAG().

    Utilise une CTE pour pré-agréger avant le fenêtrage.

    Colonnes : domaine, annee, taux_emploi_moyen, salaire_median_moyen,
               delta_taux, delta_salaire
    """
    requete = """
        WITH insertion_annuelle AS (
            SELECT
                d.libelle                              AS domaine,
                c.annee,
                AVG(i.taux_emploi_18m)                AS taux_emploi_moyen,
                AVG(i.salaire_median)                  AS salaire_median_moyen,
                COUNT(*)                               AS nb_observations
            FROM insertion_pro  i
            JOIN formation      f ON f.id_form     = i.id_form
            JOIN domaine        d ON d.id_domaine  = f.id_domaine
            JOIN campagne       c ON c.id_campagne = i.id_campagne
            WHERE i.nb_repondants >= 5
            GROUP BY d.libelle, c.annee
        )
        SELECT
            domaine,
            annee,
            ROUND(taux_emploi_moyen::numeric, 1)   AS taux_emploi_moyen,
            ROUND(salaire_median_moyen::numeric, 0) AS salaire_median_moyen,
            nb_observations,
            ROUND((taux_emploi_moyen - LAG(taux_emploi_moyen) OVER (
                PARTITION BY domaine ORDER BY annee
            ))::numeric, 1)                         AS delta_taux,
            ROUND((salaire_median_moyen - LAG(salaire_median_moyen) OVER (
                PARTITION BY domaine ORDER BY annee
            ))::numeric, 0)                         AS delta_salaire
        FROM insertion_annuelle
        ORDER BY domaine, annee
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn)


def top_formations_insertion(top_n: int = 15) -> pd.DataFrame:
    """
    Top formations par score composite :
        score = 0.4 × taux_emploi + 0.3 × (salaire/2500×100) + 0.3 × pct_cadre

    Colonnes : etablissement, formation, domaine, taux_emploi_18m,
               salaire_median, pct_emploi_cadre, score_composite
    """
    requete = """
        SELECT
            LEFT(e.nom, 55)        AS etablissement,
            LEFT(f.libelle, 70)    AS formation,
            d.libelle              AS domaine,
            f.niveau,
            i.taux_emploi_18m,
            i.salaire_median,
            i.pct_emploi_cadre,
            i.pct_temps_plein,
            i.nb_repondants,
            ROUND((
                0.4 * COALESCE(i.taux_emploi_18m, 0)
              + 0.3 * LEAST(COALESCE(i.salaire_median, 0) / 25.0, 100)
              + 0.3 * COALESCE(i.pct_emploi_cadre, 0)
            )::numeric, 1)         AS score_composite
        FROM insertion_pro  i
        JOIN formation      f ON f.id_form    = i.id_form
        JOIN etablissement  e ON e.id_etab    = f.id_etab
        JOIN domaine        d ON d.id_domaine = f.id_domaine
        WHERE i.nb_repondants  >= 30
          AND i.taux_emploi_18m IS NOT NULL
          AND i.salaire_median   IS NOT NULL
        ORDER BY score_composite DESC
        LIMIT :top_n
    """
    with obtenir_moteur().connect() as conn:
        return pd.read_sql(text(requete), conn, params={"top_n": top_n})


def stats_insertion_globales() -> dict:
    """Indicateurs clés pour les cartes du dashboard insertion."""
    requete = """
        SELECT
            COUNT(*)                                    AS nb_mesures,
            ROUND(AVG(taux_emploi_18m)::numeric, 1)     AS taux_emploi_moyen,
            ROUND(AVG(salaire_median)::numeric, 0)       AS salaire_median_moyen,
            ROUND(AVG(pct_emploi_cadre)::numeric, 1)     AS pct_cadre_moyen,
            ROUND(AVG(pct_temps_plein)::numeric, 1)      AS pct_tps_plein_moyen
        FROM insertion_pro
        WHERE nb_repondants >= 20
          AND taux_emploi_18m IS NOT NULL
    """
    with obtenir_moteur().connect() as conn:
        row = conn.execute(text(requete)).fetchone()
        return {
            "nb_mesures": int(row.nb_mesures or 0),
            "taux_emploi_moyen": float(row.taux_emploi_moyen or 0),
            "salaire_median_moyen": float(row.salaire_median_moyen or 0),
            "pct_cadre_moyen": float(row.pct_cadre_moyen or 0),
        }
