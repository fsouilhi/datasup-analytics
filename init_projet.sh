#!/bin/bash
# ============================================================
# DataSup Analytics — Initialisation de la structure du projet
# ============================================================

set -e

echo "Création de l'arborescence du projet..."

mkdir -p donnees/brutes
mkdir -p donnees/traitees
mkdir -p bdd/migrations
mkdir -p etl
mkdir -p analytique
mkdir -p dashboard/pages
mkdir -p dashboard/composants
mkdir -p tests
mkdir -p .github/workflows
mkdir -p docs/images

# Fichiers vides de base
touch etl/__init__.py
touch etl/connexion.py
touch etl/charger_parcoursup.py
touch etl/charger_insertion.py
touch etl/pipeline.py
touch analytique/__init__.py
touch analytique/requetes_parcoursup.py
touch analytique/requetes_insertion.py
touch analytique/requetes_agregees.py
touch dashboard/composants/__init__.py
touch tests/__init__.py
touch tests/test_etl.py
touch tests/test_requetes.py

# .gitignore
cat > .gitignore << 'EOF'
# Données brutes (trop volumineuses pour Git)
donnees/brutes/
donnees/traitees/

# Variables d'environnement
.env

# Python
__pycache__/
*.py[cod]
.venv/
*.egg-info/

# VS Code
.vscode/

# Logs
*.log
EOF

echo "Structure créée avec succès."
echo ""
echo "Prochaine étape : copier .env.example en .env et renseigner les variables."
