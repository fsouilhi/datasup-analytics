"""
Tests ETL — DataSup Analytics
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from pathlib import Path


# ── Tests charger_parcoursup ─────────────────────────────

class TestRenommer:
    """Tests de la fonction _renommer."""

    def test_renomme_colonnes_par_indice(self):
        from etl.charger_parcoursup import _renommer
        # Créer un DataFrame avec 113 colonnes (minimum requis)
        colonnes = [f"col_{i}" for i in range(113)]
        df = pd.DataFrame([["val"] * 113], columns=colonnes)
        resultat = _renommer(df)
        assert "uai" in resultat.columns
        assert "taux_acces" in resultat.columns
        assert "nom_etab" in resultat.columns

    def test_ajoute_colonnes_manquantes(self):
        from etl.charger_parcoursup import _renommer
        colonnes = [f"col_{i}" for i in range(10)]
        df = pd.DataFrame([["val"] * 10], columns=colonnes)
        resultat = _renommer(df)
        assert "taux_acces" in resultat.columns
        assert pd.isna(resultat["taux_acces"].iloc[0])


class TestNettoyer:
    """Tests de la fonction _nettoyer."""

    def _df_minimal(self, annee=2023):
        """Crée un DataFrame minimal avec les colonnes attendues."""
        from etl.charger_parcoursup import INDICES
        colonnes = [f"col_{i}" for i in range(113)]
        valeurs = [""] * 113
        # Remplir les colonnes clés
        valeurs[INDICES["uai"]] = "0781944P"
        valeurs[INDICES["annee"]] = str(annee)
        valeurs[INDICES["nom_etab"]] = "Université Test"
        valeurs[INDICES["taux_acces"]] = "75.0"
        valeurs[INDICES["nb_voeux"]] = "500"
        valeurs[INDICES["nb_admis"]] = "375"
        return pd.DataFrame([valeurs], columns=colonnes)

    def test_filtre_uai_invalide(self):
        from etl.charger_parcoursup import _nettoyer
        df = self._df_minimal()
        from etl.charger_parcoursup import INDICES
        df.iloc[0, INDICES["uai"]] = "INVALIDE"
        resultat = _nettoyer(df, 2023)
        assert len(resultat) == 0

    def test_conserve_uai_valide(self):
        from etl.charger_parcoursup import _nettoyer
        df = self._df_minimal()
        resultat = _nettoyer(df, 2023)
        assert len(resultat) == 1
        assert resultat.iloc[0]["uai"] == "0781944P"

    def test_annee_depuis_donnees(self):
        from etl.charger_parcoursup import _nettoyer
        # L'annee est lue depuis les donnees (col 0 = 2023), pas depuis le parametre
        df = self._df_minimal(annee=2023)
        resultat = _nettoyer(df, 2022)
        assert resultat.iloc[0]["annee"] == 2023

    def test_annee_par_defaut_si_absente(self):
        from etl.charger_parcoursup import _nettoyer
        import numpy as np
        from etl.charger_parcoursup import INDICES
        df = self._df_minimal()
        df.iloc[0, INDICES["annee"]] = ""  # Annee absente
        resultat = _nettoyer(df, 2022)
        assert resultat.iloc[0]["annee"] == 2022

    def test_taux_acces_numerique(self):
        from etl.charger_parcoursup import _nettoyer
        df = self._df_minimal()
        resultat = _nettoyer(df, 2023)
        assert pd.api.types.is_float_dtype(resultat["taux_acces"])


class TestDeduire:
    """Tests des fonctions de déduction domaine/niveau/type."""

    def test_type_etab_universite(self):
        from etl.charger_parcoursup import _type_etab
        assert _type_etab("Université de Nantes", "") == "Université"

    def test_type_etab_iut(self):
        from etl.charger_parcoursup import _type_etab
        assert _type_etab("IUT de Nantes", "") == "IUT"

    def test_type_etab_but(self):
        from etl.charger_parcoursup import _type_etab
        assert _type_etab("", "BUT Informatique") == "IUT"

    def test_niveau_licence(self):
        from etl.charger_parcoursup import _niveau
        assert _niveau("Licence - Informatique") == "Licence"

    def test_niveau_master(self):
        from etl.charger_parcoursup import _niveau
        assert _niveau("Master Informatique") == "Master"

    def test_niveau_autre(self):
        from etl.charger_parcoursup import _niveau
        assert _niveau("Formation inconnue") == "Autre"

    def test_domaine_info(self):
        from etl.charger_parcoursup import _domaine
        md = {"INFO": 1, "AUTRE": 12}
        assert _domaine("Licence Informatique", md) == 1

    def test_domaine_droit(self):
        from etl.charger_parcoursup import _domaine
        md = {"DROIT": 5, "AUTRE": 12}
        assert _domaine("Licence Droit", md) == 5


# ── Tests charger_insertion ──────────────────────────────

class TestNumInsertion:
    """Tests de la fonction _num."""

    def test_valeur_numerique(self):
        from etl.charger_insertion import _num
        assert _num("93") == 93.0

    def test_valeur_ns(self):
        from etl.charger_insertion import _num
        assert _num("ns") is None

    def test_valeur_nan(self):
        from etl.charger_insertion import _num
        assert _num(float("nan")) is None

    def test_valeur_vide(self):
        from etl.charger_insertion import _num
        assert _num("") is None

    def test_valeur_virgule(self):
        from etl.charger_insertion import _num
        assert _num("1 970") == 1970.0


# ── Tests connexion ──────────────────────────────────────

class TestConnexion:
    """Tests de la configuration de connexion."""

    def test_url_locale(self, monkeypatch):
        monkeypatch.setenv("ENV_CIBLE", "local")
        monkeypatch.setenv("PG_LOCAL_HOST", "localhost")
        monkeypatch.setenv("PG_LOCAL_PORT", "5432")
        monkeypatch.setenv("PG_LOCAL_BASE", "datasup_test")
        monkeypatch.setenv("PG_LOCAL_UTILISATEUR", "postgres")
        monkeypatch.setenv("PG_LOCAL_MOT_DE_PASSE", "test")

        # Réinitialiser le moteur singleton
        import etl.connexion as c
        c._moteur = None

        from etl.connexion import _construire_url
        url = _construire_url()
        assert "localhost" in url
        assert "datasup_test" in url
        assert "postgres" in url

    def test_erreur_sans_mot_de_passe(self, monkeypatch):
        monkeypatch.setenv("ENV_CIBLE", "local")
        monkeypatch.delenv("PG_LOCAL_MOT_DE_PASSE", raising=False)

        import etl.connexion as c
        c._moteur = None

        from etl.connexion import _construire_url
        with pytest.raises(ValueError, match="Mot de passe"):
            _construire_url()