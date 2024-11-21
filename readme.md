

Voici une version améliorée du README.md qui met l'accent sur le but spécifique du projet :

```markdown
# Flight Analytics Project - Prédiction des Prix des Vols au départ de Bordeaux

## À Propos du Projet
Ce projet vise à analyser et prédire les prix des vols au départ de l'aéroport de Bordeaux-Mérignac vers différentes destinations européennes. En utilisant des techniques de data science et de machine learning, nous cherchons à :
- Identifier les tendances de prix pour optimiser les achats de billets
- Prédire les variations de prix futures
- Comprendre les facteurs influençant les tarifs aériens

## Pipeline de Données
1. **Collecte de Données** : Scraping quotidien des prix de vols depuis Bordeaux
2. **Traitement ETL** : 
   - Nettoyage et structuration des données brutes
   - Enrichissement avec des features temporelles et tarifaires (voir `feature_builder.py`)
   - Stockage dans BigQuery pour analyse

## Features Principales
Comme illustré dans le feature builder (```python:src/data/feature_builder.py
startLine: 15
endLine: 30
```), le projet analyse :
- Prix relatifs par route
- Prix par minute de vol
- Volatilité des prix
- Caractéristiques temporelles (saison, jour de la semaine, etc.)

## Structure du Projet
```
flight_analytic/
├── src/
│   ├── data/           # Pipeline ETL et feature engineering
│   ├── models/         # Modèles de prédiction
│   └── visualization/  # Visualisations et dashboards
├── data/              
│   ├── raw/           # Données brutes par destination
│   └── processed/     # Données enrichies
└── notebooks/         # Analyses exploratoires
```

## Destinations Analysées
Le projet couvre plusieurs destinations européennes majeures au départ de Bordeaux (BOD) :
- Amsterdam (AMS)
- Athènes (ATH)
- Barcelone (BCN)
- Bruxelles (BRU)
- Dublin (DUB)
- Et d'autres destinations européennes

## Installation et Configuration
1. Cloner le repository
```bash
git clone https://github.com/Arctusol/flight_analytic.git
cd flight_analytic
```

2. Créer et activer l'environnement virtuel
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
```

3. Installer les dépendances
```bash
pip install -r requirements.txt
```

## Utilisation
1. Pipeline ETL :
```bash
python src/data/concatenator.py
```

2. Feature Engineering :
```bash
python src/data/feature_builder.py
```

3. Analyse et Modélisation :
- Notebooks Jupyter dans `notebooks/`
- Scripts de modélisation dans `src/models/`

## Roadmap
- [x] Pipeline de collecte de données
- [x] Feature engineering de base
- [ ] Modèles de prédiction
- [ ] API de prédiction
- [ ] Interface utilisateur

## Contribution
Les contributions sont bienvenues ! Pour contribuer :
1. Fork le projet
2. Créer une branche (`git checkout -b feature/NouvelleFonctionnalite`)
3. Commit (`git commit -m 'Ajout nouvelle fonctionnalité'`)
4. Push (`git push origin feature/NouvelleFonctionnalite`)
5. Ouvrir une Pull Request

## Licence


## Contact
 [@Arctusol](https://github.com/Arctusol)

Lien du projet : [https://github.com/Arctusol/flight_analytic](https://github.com/Arctusol/flight_analytic)
