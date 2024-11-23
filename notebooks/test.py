import sys
sys.path.append('..')
from src.data.feature_builder import FlightFeatureBuilder

import pandas as pd
import numpy as np  # Ajout de l'import numpy
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Initialisation
data_path = Path('../data')
feature_builder = FlightFeatureBuilder(data_path)

# Configuration du style de visualisation
sns.set_palette("husl")

# Chargement et préparation des données
df_ams = feature_builder.process_destination('AMS')

# 1. Analyse statistique des prix
print("Statistiques descriptives des prix par compagnie:")
stats_df = df_ams.groupby('airlines')['price'].describe()
print(stats_df.round(2))

# 2. Distribution des prix par compagnie
plt.figure(figsize=(12, 6))
sns.boxplot(data=df_ams, x='airlines', y='price', showfliers=False)
plt.xticks(rotation=45)
plt.title('Distribution des prix par compagnie - Amsterdam')
plt.ylabel('Prix (€)')
plt.xlabel('Compagnie aérienne')
plt.show()

# 3. Évolution temporelle des prix moyens
plt.figure(figsize=(12, 6))
df_ams_avg = df_ams.groupby(['days_until_flight', 'airlines'])['price'].mean().reset_index()
sns.lineplot(data=df_ams_avg, x='days_until_flight', y='price', hue='airlines')
plt.title('Évolution du prix moyen selon le délai de réservation - Amsterdam')
plt.xlabel('Jours avant le vol')
plt.ylabel('Prix moyen (€)')
plt.legend(title='Compagnie', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# 4. Distribution des prix selon la période de la journée
plt.figure(figsize=(10, 6))
df_ams['hour'] = pd.to_datetime(df_ams['departure_time']).dt.hour
sns.violinplot(data=df_ams, x='hour', y='price')
plt.title('Distribution des prix selon l\'heure de départ - Amsterdam')
plt.xlabel('Heure de départ')
plt.ylabel('Prix (€)')
plt.show()

# 5. Analyse des corrélations avec heatmap améliorée
correlation_vars = ['price', 'duration', 'days_until_flight', 
                   'route_competition', 'route_frequency']
correlation_matrix = df_ams[correlation_vars].corr()

plt.figure(figsize=(10, 8))
mask = np.triu(np.ones_like(correlation_matrix), k=1)
sns.heatmap(correlation_matrix, 
            mask=mask,
            annot=True, 
            cmap='coolwarm',
            center=0,
            square=True,
            fmt='.2f',
            cbar_kws={"shrink": .5})
plt.title('Matrice de corrélation des variables - Amsterdam')
plt.tight_layout()
plt.show()

# 6. Prix moyen par jour de la semaine
plt.figure(figsize=(10, 6))
df_ams['weekday'] = pd.to_datetime(df_ams['departure_time']).dt.day_name()
weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
sns.barplot(data=df_ams, x='weekday', y='price', order=weekday_order)
plt.title('Prix moyen par jour de la semaine - Amsterdam')
plt.xticks(rotation=45)
plt.xlabel('Jour de la semaine')
plt.ylabel('Prix moyen (€)')
plt.show()