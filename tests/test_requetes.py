"""
Tests requêtes analytiques — DataSup Analytics
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


# ── Tests requetes_parcoursup ────────────────────────────

class TestStatsGlobales:
    """Tests de stats_globales."""

    def test_retourne_dict(self):
        from analytique.requetes_parcoursup import stats_globales
        with patch("analytique.requetes_parcoursup.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_row = MagicMock()
            mock_row.nb_formations = 100
            mock_row.nb_etablissements = 50
            mock_row.total_voeux = 10000
            mock_row.total_admis = 5000
            mock_row.taux_acces_moyen = 50.0
            mock_conn.execute.return_value.fetchone.return_value = mock_row

            resultat = stats_globales(2023)

        assert isinstance(resultat, dict)
        assert "nb_formations" in resultat
        assert "nb_etablissements" in resultat
        assert "total_voeux" in resultat
        assert "total_admis" in resultat
        assert "taux_acces_moyen" in resultat

    def test_valeurs_numeriques(self):
        from analytique.requetes_parcoursup import stats_globales
        with patch("analytique.requetes_parcoursup.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_row = MagicMock()
            mock_row.nb_formations = 7317
            mock_row.nb_etablissements = 4057
            mock_row.total_voeux = 6816365
            mock_row.total_admis = 320668
            mock_row.taux_acces_moyen = 57.1
            mock_conn.execute.return_value.fetchone.return_value = mock_row

            resultat = stats_globales(2023)

        assert resultat["nb_formations"] == 7317
        assert resultat["taux_acces_moyen"] == 57.1

    def test_gere_valeurs_nulles(self):
        from analytique.requetes_parcoursup import stats_globales
        with patch("analytique.requetes_parcoursup.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_row = MagicMock()
            mock_row.nb_formations = None
            mock_row.nb_etablissements = None
            mock_row.total_voeux = None
            mock_row.total_admis = None
            mock_row.taux_acces_moyen = None
            mock_conn.execute.return_value.fetchone.return_value = mock_row

            resultat = stats_globales(2023)

        assert resultat["nb_formations"] == 0
        assert resultat["taux_acces_moyen"] == 0.0


class TestClassementSelectivite:
    """Tests de classement_selectivite."""

    def test_retourne_dataframe(self):
        from analytique.requetes_parcoursup import classement_selectivite
        with patch("analytique.requetes_parcoursup.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value = MagicMock()
            with patch("analytique.requetes_parcoursup.pd.read_sql") as mock_sql:
                mock_sql.return_value = pd.DataFrame({
                    "etablissement": ["Univ Test"],
                    "domaine": ["Informatique"],
                    "taux_acces": [10.0],
                    "rang_selectivite": [1],
                    "annee": [2023],
                })
                resultat = classement_selectivite(2023, limite=1)

        assert isinstance(resultat, pd.DataFrame)
        assert len(resultat) == 1


# ── Tests requetes_insertion ─────────────────────────────

class TestStatsInsertionGlobales:
    """Tests de stats_insertion_globales."""

    def test_retourne_dict_complet(self):
        from analytique.requetes_insertion import stats_insertion_globales
        with patch("analytique.requetes_insertion.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_row = MagicMock()
            mock_row.nb_mesures = 474
            mock_row.taux_emploi_moyen = 90.9
            mock_row.salaire_median_moyen = 1652.0
            mock_row.pct_cadre_moyen = 75.8
            mock_conn.execute.return_value.fetchone.return_value = mock_row

            resultat = stats_insertion_globales()

        assert isinstance(resultat, dict)
        assert resultat["nb_mesures"] == 474
        assert resultat["taux_emploi_moyen"] == 90.9

    def test_cles_presentes(self):
        from analytique.requetes_insertion import stats_insertion_globales
        with patch("analytique.requetes_insertion.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_row = MagicMock()
            mock_row.nb_mesures = 0
            mock_row.taux_emploi_moyen = None
            mock_row.salaire_median_moyen = None
            mock_row.pct_cadre_moyen = None
            mock_conn.execute.return_value.fetchone.return_value = mock_row

            resultat = stats_insertion_globales()

        cles_attendues = {"nb_mesures", "taux_emploi_moyen",
                          "salaire_median_moyen", "pct_cadre_moyen"}
        assert cles_attendues.issubset(resultat.keys())


class TestSalairesParDomaine:
    """Tests de salaires_par_domaine."""

    def test_retourne_dataframe(self):
        from analytique.requetes_insertion import salaires_par_domaine
        with patch("analytique.requetes_insertion.obtenir_moteur") as mock_moteur:
            mock_conn = MagicMock()
            mock_moteur.return_value.connect.return_value.__enter__.return_value = mock_conn
            with patch("analytique.requetes_insertion.pd.read_sql") as mock_sql:
                mock_sql.return_value = pd.DataFrame({
                    "domaine": ["Informatique", "Droit"],
                    "secteur": ["Sciences", "Droit & Éco"],
                    "nb_formations": [17, 8],
                    "salaire_median_moyen": [1662.0, 1903.0],
                    "taux_emploi_moyen": [None, None],
                    "pct_cadre_moyen": [91.8, 79.1],
                    "pct_temps_plein_moyen": [98.6, 94.0],
                })
                resultat = salaires_par_domaine()

        assert isinstance(resultat, pd.DataFrame)
        assert "domaine" in resultat.columns
        assert "salaire_median_moyen" in resultat.columns