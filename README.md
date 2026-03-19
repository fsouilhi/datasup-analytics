---
title: DataSup Analytics
emoji: 🎓
colorFrom: blue
colorTo: indigo
sdk: streamlit
sdk_version: "1.35.0"
app_file: dashboard/app.py
pinned: false
---


# DataSup Analytics

**Observatoire analytique de l'enseignement supérieur français**

Pipeline ETL complet, modélisation PostgreSQL relationnelle, requêtes SQL analytiques avancées (fenêtrage, CTEs, agrégations) et dashboard interactif Streamlit/Plotly.

[![CI](https://github.com/fsouilhi/datasup-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/fsouilhi/datasup-analytics/actions)

## Liens

- **Repo GitHub** : [github.com/fsouilhi/datasup-analytics](https://github.com/fsouilhi/datasup-analytics)
- **Dashboard en ligne** : [huggingface.co/spaces/fsouilhi/datasup-analytics](https://huggingface.co/spaces/fsouilhi/datasup-analytics)

---

## Aperçu

| Indicateur | Valeur |
|---|---|
| Formations analysées | 7 317 |
| Établissements | 4 057 |
| Vœux Parcoursup 2023 | 6 816 365 |
| Admis 2023 | 320 668 |
| Mesures d'insertion pro | 1 314 |

---

## Stack technique

| Couche | Technologies |
|---|---|
| Base de données | PostgreSQL 15, Supabase |
| Modélisation | Merise MCD/MLD, DDL avec contraintes et index |
| ETL | Python, pandas, SQLAlchemy, psycopg2 |
| SQL analytique | CTEs, RANK(), ROW_NUMBER(), LAG(), agrégations |
| Dashboard | Streamlit, Plotly Express, Plotly Graph Objects |
| Tests | pytest, pytest-cov |
| CI/CD | GitHub Actions |
| Déploiement | HuggingFace Spaces |

---

## Architecture
```
datasup-analytics/
├── bdd/schema.sql              # DDL PostgreSQL — 6 tables, contraintes, index
├── donnees/
│   ├── telecharger.py          # Téléchargement automatique data.gouv.fr / MESR
│   └── explorer.py             # Exploration et validation des datasets
├── etl/
│   ├── connexion.py            # Pool SQLAlchemy — local et Supabase
│   ├── charger_parcoursup.py   # ETL Parcoursup 2021-2023
│   ├── charger_insertion.py    # ETL insertion pro Master et Licence Pro
│   └── pipeline.py             # Orchestrateur ETL
├── analytique/
│   ├── requetes_parcoursup.py  # RANK(), ROW_NUMBER(), LAG(), CTEs
│   └── requetes_insertion.py   # Score composite, évolution temporelle
├── dashboard/
│   ├── app.py                  # Page d'accueil — métriques globales
│   └── pages/
│       ├── 01_parcoursup.py    # Sélectivité, profil admis, évolution
│       └── 02_insertion.py     # Salaires, taux emploi, top formations
└── tests/
    ├── test_etl.py
    └── test_requetes.py
```

---

## Modèle de données
```
etablissement ──< formation >── domaine
                     │
               ┌─────┴──────┐
            parcoursup   insertion_pro
               │                │
            campagne         campagne
```

6 tables · contraintes CHECK · clés étrangères · 11 index analytiques

---

## Installation locale

**Prérequis :** Python 3.11+, PostgreSQL 15+
```bash
git clone https://github.com/fsouilhi/datasup-analytics.git
cd datasup-analytics
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
sudo -u postgres createdb datasup
sudo -u postgres psql -d datasup -f bdd/schema.sql
python donnees/telecharger.py
python -m etl.pipeline
PYTHONPATH=$(pwd) streamlit run dashboard/app.py
```

---

## Sources de données

| Dataset | Source | Périmètre |
|---|---|---|
| Parcoursup 2021-2023 | [data.gouv.fr](https://www.data.gouv.fr) | ~42 000 lignes |
| Insertion pro Master | [MESR](https://data.enseignementsup-recherche.gouv.fr) | 19 603 lignes |
| Insertion pro Licence Pro | [MESR](https://data.enseignementsup-recherche.gouv.fr) | 11 780 lignes |

---

## Auteure

**Fatima Souilhi**
[github.com/fsouilhi](https://github.com/fsouilhi)
