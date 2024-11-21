import pandas as pd
import numpy as np
from typing import Dict, List
from pathlib import Path

class FlightFeatureBuilder:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        
    def load_destination_data(self, destination: str) -> pd.DataFrame:
        """Charge les données CSV d'une destination"""
        file_path = self.data_path / 'combined' / f'vols_{destination.lower()}_combines.csv'
        return pd.read_csv(file_path)
    
    def add_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ajoute des features liées aux prix"""
        df = df.copy()
        
        # Prix moyen par route
        route_avg = df.groupby(['origin', 'destination'])['price'].transform('mean')
        df['price_vs_route_avg'] = df['price'] / route_avg
        
        # Prix par minute de vol
        df['price_per_minute'] = df['price'] / df['duration']
        
        # Volatilité des prix
        price_std = df.groupby(['origin', 'destination'])['price'].transform('std')
        df['price_volatility'] = price_std / route_avg
        
        return df
    
    def add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ajoute des features temporelles"""
        df = df.copy()
        
        # Conversion des dates
        df['search_date'] = pd.to_datetime(df['search_date'])
        df['flight_date'] = pd.to_datetime(df['flight_date'])
        
        # Features temporelles
        df['search_month'] = df['search_date'].dt.month
        df['flight_month'] = df['flight_date'].dt.month
        df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
        
        # Saisons
        season_map = {12: 'Winter', 1: 'Winter', 2: 'Winter',
                     3: 'Spring', 4: 'Spring', 5: 'Spring',
                     6: 'Summer', 7: 'Summer', 8: 'Summer',
                     9: 'Fall', 10: 'Fall', 11: 'Fall'}
        df['season'] = df['flight_month'].map(season_map)
        
        return df
    
    def add_route_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ajoute des features liées aux routes"""
        df = df.copy()
        
        # Nombre de compagnies par route
        route_airlines = df.groupby(['origin', 'destination'])['airlines'].transform('nunique')
        df['route_competition'] = route_airlines
        
        # Fréquence des vols
        route_frequency = df.groupby(['origin', 'destination']).size().reset_index(name='route_frequency')
        df = df.merge(route_frequency, on=['origin', 'destination'])
        
        return df
    
    def process_destination(self, destination: str) -> pd.DataFrame:
        """Traitement complet pour une destination"""
        df = self.load_destination_data(destination)
        
        df = (df
            .pipe(self.add_price_features)
            .pipe(self.add_temporal_features)
            .pipe(self.add_route_features)
        )
        
        return df