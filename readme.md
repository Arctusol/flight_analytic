# Flight Analytics Project 🛫
> Prédiction des prix des vols au départ de Bordeaux

## 📋 À Propos
Ce projet vise à analyser et prédire les prix des vols au départ de l'aéroport de Bordeaux-Mérignac vers différentes destinations européennes. En utilisant des techniques de data science et de machine learning, nous cherchons à :
- Identifier les tendances de prix pour optimiser les achats de billets
- Prédire les variations de prix futures
- Comprendre les facteurs influençant les tarifs aériens

### Fonctionnalités Principales
- 📊 Analyse des tendances de prix en temps réel
- 🔮 Prédiction des variations tarifaires
- 📈 Identification des facteurs influençant les prix
- 🗺️ Couverture de multiples destinations européennes

## 🚀 Démarrage Rapide

### Prérequis
- Python 3.8+
- pip
- git

### Installation
```bash
# Cloner le projet
git clone https://github.com/Arctusol/flight_analytic.git
cd flight_analytic

# Créer l'environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## 🛠️ Pipeline de Données
1. **Scraping** : `python Scrapper.py`
2. **Transformation to CSV** : `python src/data/concatenator.py`
3. **Pipeline to BigQuery** : `python src/data/automated_pipeline.py`
4. **DBT structuration** : 
4. **Analyse** : Voir notebooks dans `notebooks/`

## 🌍 Destinations Couvertes

| Ville | Code IATA |
|-------|-----------|
| Amsterdam | AMS |
| Athènes | ATH |
| Barcelone | BCN |
| Bruxelles | BRU |
| Dublin | DUB |
| Londres | LON |
| *Et plus encore...* | |

## 📁 Structure du Projet
```
flight_analytic/
├── src/
│ ├── data/ # ETL et features
│ │ ├── init.py
│ │ ├── concatenator.py # Fusion des données
│ │ ├── feature_builder.py # Création des features
│ │ ├── automated_pipeline.py # Pipeline BigQuery
│ │ ├── compting_lines.py # Utilitaire statistiques
│ │ └── test_proxies.ps1 # Test des proxies
│ │
│ ├── models/ # Modèles ML
│ │ ├── init.py
│ │ └── predictor.py # Prédiction des prix
│ │
│ ├── analysis/ # Analyses des données
│ │ ├── init.py
│ │ ├── price_analyzer.py # Analyse des prix
│ │ └── route_analyzer.py # Analyse des routes
│ │
│ ├── visualization/ # Visualisations
│ │ ├── init.py
│ │ └── plotter.py # Graphiques
│ │
│ └── dashboard/ # Interface utilisateur
│ └── app.py # Application Streamlit
│
├── data/ # Données
│ ├── raw/ # Données brutes
│ ├── processed/ # Données traitées
│ ├── combined/ # Données fusionnées
│ ├── logs/ # Fichiers de logs
│ └── metrics/ # Métriques de performance
│
├── notebooks/ # Analyses Jupyter
│ ├── 01_price_analysis.ipynb # Analyse des prix
│ ├── 02_route_analysis.ipynb # Analyse des routes
│ ├── 03_prediction.ipynb # Modèles prédictifs
│ └── 04_modeling.ipynb # Modélisation avancée

```

## 📊 Features Analysées
- Prix relatifs par route
- Prix/minute de vol
- Volatilité des prix
- Facteurs temporels (saison, jour, etc.)

## 🗺️ Roadmap
- [x] Pipeline de collecte
- [x] Feature engineering
- [ ] Modèles prédictifs
- [ ] API REST
- [ ] Interface utilisateur

## 🤝 Contribution
1. Fork
2. Créer une branche (`git checkout -b feature/amelioration`)
3. Commit (`git commit -m 'Ajout amelioration'`)
4. Push (`git push origin feature/amelioration`)
5. Pull Request

## 📫 Contact
[@Arctusol](https://github.com/Arctusol)

[Lien du projet](https://github.com/Arctusol/flight_analytic)
