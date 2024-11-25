import json
import pandas as pd
import os
from pathlib import Path
import csv
import logging
from datetime import datetime
import time
from contextlib import contextmanager
import shutil
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from functools import lru_cache


# Coordonnées fixes de Bordeaux
BORDEAUX_COORDS = (44.837789, -0.57918)  # Latitude, Longitude

# Déplacer le dictionnaire city_country_mapping au niveau global pour le rendre accessible partout
city_country_mapping = {
    'London': 'United Kingdom',
    'Roma': 'Italy', 
    'Madrid': 'Spain',
    'Lisboa': 'Portugal',
    'Amsterdam': 'Netherlands',
    'Dublin': 'Ireland',
    'Berlin': 'Germany',
    'Wien': 'Austria',
    'Bruxelles': 'Belgium',
    'Barcelona': 'Spain',
    'Copenhagen': 'Denmark',
    'Prague': 'Czech Republic',
    'Athens': 'Greece',
    'Warsaw': 'Poland',
    'Budapest': 'Hungary',
    'Zurich': 'Switzerland',
    'Oslo': 'Norway', 
    'Stockholm': 'Sweden',
    'Helsinki': 'Finland',
    'Istanbul': 'Turkey',
    'Milan': 'Italy',
    'New York': 'United States',
    'Los Angeles': 'United States',
    'San Francisco': 'United States',
    'Miami': 'United States',
    'Chicago': 'United States',
    'Montreal': 'Canada',
    'Toronto': 'Canada',
    'Vancouver': 'Canada',
    'Mexico City': 'Mexico',
    'Cancun': 'Mexico',
    'Sao Paulo': 'Brazil',
    'Buenos Aires': 'Argentina',
    'Santiago': 'Chile',
    'Bogota': 'Colombia',
    'Lima': 'Peru',
    'Rio de Janeiro': 'Brazil',
    'Dubai': 'United Arab Emirates',
    'Doha': 'Qatar',
    'Abu Dhabi': 'United Arab Emirates',
    'Singapore': 'Singapore',
    'Hong Kong': 'China',
    'Bangkok': 'Thailand',
    'Kuala Lumpur': 'Malaysia',
    'Tokyo': 'Japan',
    'Seoul': 'South Korea',
    'Beijing': 'China',
    'Shanghai': 'China',
    'New Delhi': 'India',
    'Mumbai': 'India',
    'Sydney': 'Australia',
    'Melbourne': 'Australia',
    'Auckland': 'New Zealand',
    'Brisbane': 'Australia',
    'Perth': 'Australia',
    'Johannesburg': 'South Africa',
    'Cape Town': 'South Africa',
    'Cairo': 'Egypt',
    'Casablanca': 'Morocco',
    'Dakar': 'Senegal',
    'Nairobi': 'Kenya'
}

@lru_cache(maxsize=128)
def get_city_coordinates(city_name):
    """
    Obtient les coordonnées d'une ville avec mise en cache.
    
    Args:
        city_name (str): Nom de la ville
        
    Returns:
        tuple: (latitude, longitude) ou None si non trouvé
    """
    if not city_name or city_name == 'N/A':
        return None
    
    try:
        geolocator = Nominatim(user_agent="flight_scraper")
        
        # Utiliser le mapping ville-pays
        if city_name in city_country_mapping:
            search_query = f"{city_name}, {city_country_mapping[city_name]}"
        else:
            search_query = f"{city_name}, France"  # Par défaut
            logger.warning(f"Ville non trouvée dans le mapping: {city_name}, recherche avec pays par défaut")
            
        logger.debug(f"Recherche de géolocalisation pour: {search_query}")
        location = geolocator.geocode(search_query)
        
        if location:
            logger.debug(f"Coordonnées trouvées pour {search_query}: {location.latitude}, {location.longitude}")
            return (location.latitude, location.longitude)
            
        logger.warning(f"Aucune coordonnée trouvée pour {search_query}")
        return None
        
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        logger.error(f"Erreur de géocodage pour {city_name}: {str(e)}")
        return None

def format_coordinates(coords):
    """
    Formate les coordonnées en chaîne de caractères.
    """
    if coords is None:
        return None
    return f"{coords[0]}, {coords[1]}"

def calculate_distance_and_coordinates(row):
    """
    Calcule la distance et récupère les coordonnées pour une ligne du DataFrame.
    
    Returns:
        dict: Dictionnaire contenant les coordonnées et la distance
    """
    origin_coords = BORDEAUX_COORDS
    dest_coords = get_city_coordinates(row['destination_city'])
    
    result = {
        'origin_coordinates': format_coordinates(origin_coords),
        'destination_coordinates': format_coordinates(dest_coords),
        'distance_km': None
    }
    
    if origin_coords and dest_coords:
        result['distance_km'] = round(geodesic(origin_coords, dest_coords).kilometers, 2)
    
    return pd.Series(result)

# Configuration du logging
def setup_logger(base_folder):
    # Créer un dossier pour les logs s'il n'existe pas
    log_folder = base_folder / 'logs'
    log_folder.mkdir(exist_ok=True)
    
    # Créer un nom de fichier avec la date
    log_filename = log_folder / f'concatenator_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    # Configuration du logger
    logger = logging.getLogger('concatenator')
    logger.setLevel(logging.INFO)
    
    # Handler pour fichier
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Handler pour console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Format du log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Ajout des handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialisation du logger (à mettre après la définition de base_folder)
base_folder = Path(r"C:\Users\antob\Documents\Arctusol\Scrapping_project\Scraping\data")
logger = setup_logger(base_folder)

def clean_time_format(time_str):
    if not isinstance(time_str, str) or time_str == 'N/A':
        return 'N/A'
    # Remplace tous les types de tirets par un tiret simple
    cleaned_str = time_str.replace('–', '-').replace('\u2013', '-').replace('\u2014', '-')
    # Supprime les espaces autour du tiret
    cleaned_str = ' - '.join(part.strip() for part in cleaned_str.split('-'))
    return cleaned_str

def split_time(time_str):
    if not isinstance(time_str, str) or time_str == 'N/A':
        return 'N/A', 'N/A'
    parts = time_str.split(' - ')
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return 'N/A', 'N/A'

class BackupManager:
    def __init__(self, base_path):
        self.backup_path = base_path / 'backups'
        self.backup_path.mkdir(exist_ok=True)

    def create_backup(self, file_path):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.backup_path / f"{file_path.stem}_{timestamp}{file_path.suffix}"
        shutil.copy2(file_path, backup_file)
        return backup_file
    
def clean_price(price, destination, file_name):
    try:
        if pd.isna(price):
            return None
        if isinstance(price, (int, float)):
            return float(price)
        if isinstance(price, str):
            # Log la valeur problématique avant le nettoyage
            logger.debug(f"[{destination}] Nettoyage du prix: '{price}' dans {file_name}")
            
            # Remplacer tous les types d'espaces Unicode
            price = price.replace('€', '').replace('\xa0', '').replace('\u202f', '').replace(' ', '').strip()
            logger.debug(f"[{destination}] Prix après nettoyage: '{price}' dans {file_name}")
            
            return float(price) if price else None
        return None
    except Exception as e:
        # Ajouter plus de contexte dans le message d'erreur
        logger.error(f"[{destination}] Erreur dans le nettoyage du prix '{price}' dans {file_name}: {str(e)}")
        return None

def flatten_flight_data(json_data, file_name, destination):
    logger.info(f"[{destination}] Début du traitement du fichier {file_name}")
    flattened_data = []
    
    try:
        base_data = {
            'search_date': json_data.get('search_date', 'N/A'),
            'flight_date': json_data.get('flight_date', 'N/A'),
            'origin': json_data.get('origin', 'N/A'),
            'destination': json_data.get('destination', 'N/A'),
            'destination_city': json_data.get('destination_city', 'N/A'),
            'url': json_data.get('url', 'N/A')
        }
        
        logger.debug(f"Données de base extraites pour {file_name}: {base_data}")
        
        for idx, flight in enumerate(json_data['flights'], 1):
            try:
                # Ajouter un log pour chaque vol traité avec son prix
                logger.debug(f"[{destination}] Traitement du vol {idx} dans {file_name} - Prix brut: {flight.get('price', 'N/A')}")
                
                row = base_data.copy()
                departure_time, arrival_time = split_time(clean_time_format(flight.get('departure_time', 'N/A')))
                
                # Gestion des compagnies aériennes
                airlines = flight.get('airlines', [])
                main_airline = airlines[0] if airlines else 'N/A'
                connection_airline = airlines[1] if len(airlines) > 1 else 'N/A'
                
                flight_data = {
                    'departure_time': departure_time,
                    'arrival_time': arrival_time,
                    'duration': flight.get('duration', 'N/A'),
                    'price': clean_price(flight.get('price', 'N/A'), destination, file_name),
                    'is_direct': flight.get('is_direct', 'N/A'),
                    'origin_airport': flight.get('origin_airport', 'N/A'),
                    'destination_airport': flight.get('destination_airport', 'N/A'),
                    'checked_baggage': flight.get('checked_baggage', 'N/A'),
                    'hand_baggage': flight.get('hand_baggage', 'N/A'),
                    'layover_airport': flight.get('layover_airport', 'N/A'),
                    'layover_duration': flight.get('layover_duration', 'N/A'),
                    'airlines': main_airline,
                    'flight_connection_company': connection_airline,
                    'fare_class': flight.get('fare_class', 'N/A'),
                    'origin_coordinates': format_coordinates(BORDEAUX_COORDS),
                    'destination_coordinates': format_coordinates(get_city_coordinates(row['destination_city'])),
                    'distance_km': round(geodesic(BORDEAUX_COORDS, get_city_coordinates(row['destination_city'])).kilometers, 2) 
                                 if get_city_coordinates(row['destination_city']) else None
                }
                row.update(flight_data)
                flattened_data.append(row)
                logger.debug(f"[{destination}] Vol {idx} traité avec succès dans {file_name}")
            except Exception as e:
                logger.error(f"[{destination}] Erreur lors du traitement du vol {idx} dans {file_name}: {str(e)}")
        
        logger.info(f"[{destination}] Traitement terminé pour {file_name}. {len(flattened_data)} vols extraits")
        return flattened_data
    except Exception as e:
        logger.error(f"[{destination}] Erreur lors du traitement global du fichier {file_name}: {str(e)}")
        return []

# Remplacer la partie du traitement des fichiers par :
base_folder = Path(r"C:\Users\antob\Documents\Arctusol\Scrapping_project\Scraping\data")
output_folder = base_folder / 'final_csv'  # Nouveau dossier de sortie

# Création du dossier s'il n'existe pas
output_folder.mkdir(exist_ok=True)

# Trouver automatiquement tous les dossiers commençant par 3 lettres
destination_folders = [folder.name for folder in base_folder.iterdir() 
                      if folder.is_dir() and folder.name[:3].isalpha()]

logger.info(f"Dossiers de destination trouvés : {', '.join(destination_folders)}")

for destination in destination_folders:
    destination_path = base_folder / destination
    logger.info(f"\n=== Début du traitement du dossier {destination} ===")
    
    all_data = []
    
    for json_file in destination_path.glob('*.json'):
        file_name = json_file.name
        logger.info(f"[{destination}] === Début du traitement de {file_name} ===")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                logger.debug(f"[{destination}] Fichier {file_name} chargé avec succès")
                
                flattened_data = flatten_flight_data(json_data, file_name, destination)
                all_data.extend(flattened_data)
                
                logger.info(f"[{destination}] === Fin du traitement de {file_name} ===")
                logger.info(f"[{destination}] Nombre de vols extraits: {len(flattened_data)}")
        except json.JSONDecodeError as e:
            logger.error(f"[{destination}] Erreur de décodage JSON pour {file_name}: {str(e)}")
        except Exception as e:
            logger.error(f"[{destination}] Erreur inattendue lors du traitement de {file_name}: {str(e)}")

    if not all_data:
        logger.warning(f"Aucune donnée n'a été chargée pour {destination}.")
        continue

    df = pd.DataFrame(all_data)
    
    print(f"Colonnes disponibles dans le DataFrame : {df.columns.tolist()}")

    def clean_duration(duration):
        if not isinstance(duration, str) or duration == 'N/A':
            return None
        try:
            # Convertit "6h 20min" en minutes
            parts = duration.lower().replace('h', ' ').replace('min', '').split()
            total_minutes = int(parts[0]) * 60 + (int(parts[1]) if len(parts) > 1 else 0)
            return total_minutes
        except:
            return None

    df['duration'] = df['duration'].apply(clean_duration)

    def format_dates(date_str, keep_time=False):
        if not isinstance(date_str, str) or date_str == 'N/A':
            return None
        try:
            if keep_time:
                return pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
        except:
            return None

    # Modification de l'application du format de date
    df['search_date_with_hour'] = df['search_date'].apply(lambda x: format_dates(x, keep_time=True))
    df['search_date'] = df['search_date'].apply(format_dates)
    df['flight_date'] = df['flight_date'].apply(format_dates)

    def clean_boolean(value):
        if isinstance(value, bool):
            return value
        if value == 'true':
            return True
        if value == 'false':
            return False
        return None

    df['is_direct'] = df['is_direct'].apply(clean_boolean)

    def parse_layover_duration(duration):
        if not isinstance(duration, str) or duration == 'N/A':
            return None
        try:
            # Convertit "2h 30min" en minutes
            parts = duration.lower().replace('h', ' ').replace('min', '').split()
            total_minutes = int(parts[0]) * 60 + (int(parts[1]) if len(parts) > 1 else 0)
            return total_minutes
        except:
            return None

    def create_datetime_with_time(date_str, time_str):
        if date_str == 'N/A' or time_str == 'N/A':
            return None
        try:
            # Vérifier si le temps contient "+1"
            next_day = '+1' in time_str
            time_str = time_str.replace('+1', '').strip()
            
            # Créer l'objet datetime
            base_date = pd.to_datetime(date_str)
            if next_day:
                base_date = base_date + pd.Timedelta(days=1)
            
            time_parts = time_str.split(':')
            final_datetime = base_date.replace(
                hour=int(time_parts[0]),
                minute=int(time_parts[1])
            )
            return final_datetime.strftime('%Y-%m-%d %H:%M:00')
        except:
            return None

    # Conversion de layover_duration en minutes
    df['layover_duration'] = df['layover_duration'].apply(parse_layover_duration)

    # Création des colonnes datetime
    df['flight_date_with_time'] = df.apply(
        lambda row: create_datetime_with_time(row['flight_date'], row['departure_time']), 
        axis=1
    )
    df['flight_arrival_with_time'] = df.apply(
        lambda row: create_datetime_with_time(row['flight_date'], row['arrival_time']), 
        axis=1
    )

    def calculate_second_flight_duration(row):
        if row['is_direct'] or pd.isna(row['duration']) or pd.isna(row['layover_duration']):
            return None
        try:
            total_duration = row['duration']
            layover_duration = row['layover_duration']
            second_flight = total_duration - layover_duration
            return second_flight if second_flight >= 0 else None
        except Exception as e:
            print(f"Erreur dans le calcul de second_flight_duration: {str(e)}")
            return None

    # Ajout de la nouvelle colonne
    df['second_flight_duration'] = df.apply(calculate_second_flight_duration, axis=1)

    # Calcul du délai entre la recherche et le vol
    df['days_until_flight'] = (pd.to_datetime(df['flight_date']) - pd.to_datetime(df['search_date'])).dt.days

    # Ajout du jour de la semaine
    df['day_of_week'] = pd.to_datetime(df['flight_date']).dt.day_name()

    # Classification des vols par période
    def get_time_period(time):
        if pd.isna(time):
            return None
        hour = pd.to_datetime(time).hour
        if 5 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 17:
            return 'afternoon'
        elif 17 <= hour < 22:
            return 'evening'
        else:
            return 'night'

    df['departure_period'] = df['flight_date_with_time'].apply(get_time_period)

    # Classification des vols par durée
    def classify_flight_duration(duration):
        if pd.isna(duration):
            return None
        elif duration <= 90:  # 1h30
            return 'très_court'
        elif duration <= 180:  # 3h
            return 'court'
        elif duration <= 360:  # 6h
            return 'moyen'
        elif duration <= 720:  # 12h
            return 'long'
        else:
            return 'très_long'

    df['flight_type'] = df['duration'].apply(classify_flight_duration)

    # Classification des prix
    df['price_category'] = pd.qcut(df['price'].fillna(-1), 
                                 q=3, 
                                 labels=['cheap', 'standard', 'high'])
    df.loc[df['price'] == -1, 'price_category'] = None

    # Conversion des colonnes en types appropriés
    df['is_direct'] = df['is_direct'].astype('boolean')
    df['price'] = df['price'].astype('float32')
    df['duration'] = df['duration'].astype('Int64')  # Int64 permet les valeurs NULL

    # Créer un dossier 'combined' s'il n'existe pas
    output_folder = base_folder / 'combined'
    output_folder.mkdir(exist_ok=True)
    
    # Sauvegarder dans le dossier 'combined'
    current_date = datetime.now().strftime('%Y%m%d')
    output_file = output_folder / f'vols_{destination.lower()}_combines.csv'

    # Avant la sauvegarde du CSV, ajoutons un ID unique
    def generate_flight_id(row):
        try:
            # Extraire seulement la date de search_date (format YYYY-MM-DD)
            search_date = str(row.get('search_date', '')).split()[0] if row.get('search_date') else ''
            
            components = [
                search_date,  # Uniquement la date de recherche, sans l'heure
                str(row.get('flight_date', '')),
                str(row.get('origin', '')),
                str(row.get('destination', '')),
                str(row.get('departure_time', '')),
                str(row.get('arrival_time', ''))
            ]
            unique_string = '_'.join(filter(None, components))
            return abs(hash(unique_string)) if unique_string else None
        except Exception as e:
            print(f"Erreur dans la génération de l'ID: {str(e)}")
            return None

    df['flight_id'] = df.apply(generate_flight_id, axis=1)

    # Pour gérer l'ajout à un fichier existant
    if output_file.exists():
        try:
            backup_manager = BackupManager(base_folder)
            backup_file = backup_manager.create_backup(output_file)
            logger.info(f"Backup créé: {backup_file}")
            
            # Charger le fichier existant
            existing_df = pd.read_csv(output_file)
            initial_rows = len(existing_df)
            
            # Régénérer les IDs pour le fichier existant avec la nouvelle méthode
            logger.info("Régénération des IDs pour les données existantes...")
            existing_df['flight_id'] = existing_df.apply(generate_flight_id, axis=1)
            
            # Générer les IDs pour les nouvelles données
            df['flight_id'] = df.apply(generate_flight_id, axis=1)
            
            # Concaténer et dédupliquer
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['flight_id'], keep='last')
            final_rows = len(df)
            
            logger.info(f"[{destination}] Fusion avec fichier existant: {initial_rows} -> {final_rows} lignes")
            logger.info(f"[{destination}] Nombre d'IDs uniques: {df['flight_id'].nunique()}")
            
        except pd.errors.EmptyDataError:
            logger.warning(f"Le fichier {output_file} est vide, création d'un nouveau fichier")
        except Exception as e:
            logger.error(f"Erreur lors de la lecture du fichier existant: {str(e)}")
            backup_file = output_file.with_suffix('.backup.csv')
            df.to_csv(backup_file, index=False, encoding='utf-8')
            logger.info(f"Backup créé: {backup_file}")

    # Avant la sauvegarde du CSV, supprimer les colonnes non désirées
    columns_to_drop = ['flight_number', 'equipment_type']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # Calcul des coordonnées et distances
    logger.info("Calcul des coordonnées et distances...")
    # Créer les nouvelles colonnes une par une
    df['origin_coordinates'] = df.apply(lambda row: format_coordinates(BORDEAUX_COORDS), axis=1)
    df['destination_coordinates'] = df.apply(lambda row: format_coordinates(get_city_coordinates(row['destination_city'])), axis=1)
    df['distance_km'] = df.apply(lambda row: round(geodesic(BORDEAUX_COORDS, get_city_coordinates(row['destination_city'])).kilometers, 2) 
                                if get_city_coordinates(row['destination_city']) else None, axis=1)

    # Sauvegarder le fichier avec toutes les colonnes
    df.to_csv(output_file, 
              index=False, 
              encoding='utf-8',
              quoting=csv.QUOTE_ALL,
              escapechar='\\',
              doublequote=True)

    logger.info(f"Les données ont été combinées et sauvegardées dans {output_file}")
    logger.info(f"Nombre total de vols pour {destination}: {len(df)}")
    logger.info(f"Colonnes du DataFrame: {df.columns.tolist()}")
    logger.info(f"Types de données:\n{df.dtypes}")
    
    # Statistiques de qualité des données
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning("Valeurs manquantes détectées:")
        for col, count in null_counts[null_counts > 0].items():
            logger.warning(f"  {col}: {count} valeurs manquantes")

    # Log des statistiques de base pour les colonnes numériques
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_cols:
        stats = df[col].describe()
        logger.info(f"Statistiques pour {col}:\n{stats}")

    # Log des statistiques
    logger.info("Statistiques des nouvelles colonnes:")
    for col in ['origin_coordinates', 'destination_coordinates', 'distance_km']:
        non_null_count = df[col].count()
        total_count = len(df)
        logger.info(f"{col}: {non_null_count}/{total_count} valeurs non nulles")

    distance_stats = df['distance_km'].describe()
    logger.info(f"Statistiques des distances:\n{distance_stats}")

@contextmanager
def timer(description):
    start = time.time()
    yield
    elapsed_time = time.time() - start
    logger.info(f"{description}: {elapsed_time:.2f} secondes")

# Utilisation dans le code
with timer("Traitement complet du dossier"):
    # Votre code de traitement ici
    pass

class ProcessMetrics:
    def __init__(self, base_folder):
        self.metrics_folder = base_folder / 'metrics'
        self.metrics_folder.mkdir(exist_ok=True)
        self.start_time = time.time()
        self.metrics = {
            'processed_files': 0,
            'error_count': 0,
            'total_rows': 0,
            'destinations': {},
            'processing_time': 0,
            'memory_usage': 0,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'errors': [],
            'warnings': []
        }

    def update(self, destination=None, processed=0, errors=0, rows=0):
        self.metrics['processed_files'] += processed
        self.metrics['error_count'] += errors
        self.metrics['total_rows'] += rows
        
        # Métriques par destination
        if destination:
            if destination not in self.metrics['destinations']:
                self.metrics['destinations'][destination] = {
                    'processed_files': 0,
                    'error_count': 0,
                    'total_rows': 0
                }
            self.metrics['destinations'][destination]['processed_files'] += processed
            self.metrics['destinations'][destination]['error_count'] += errors
            self.metrics['destinations'][destination]['total_rows'] += rows

    def add_error(self, error_message):
        self.metrics['errors'].append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'message': str(error_message)
        })

    def add_warning(self, warning_message):
        self.metrics['warnings'].append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'message': str(warning_message)
        })

    def save_metrics(self):
        # Calculer le temps total
        self.metrics['processing_time'] = time.time() - self.start_time
        
        # Nom du fichier avec timestamp
        metrics_file = self.metrics_folder / f'metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        # Sauvegarder les métriques
        with open(metrics_file, 'w', encoding='utf-8') as f:
            json.dump(self.metrics, f, indent=4, ensure_ascii=False)
        
        # Créer un résumé pour le logger
        logger.info("=== Rapport de traitement ===")
        logger.info(f"Temps total: {self.metrics['processing_time']:.2f} secondes")
        logger.info(f"Fichiers traités: {self.metrics['processed_files']}")
        logger.info(f"Erreurs rencontrées: {self.metrics['error_count']}")
        logger.info(f"Lignes totales: {self.metrics['total_rows']}")
        
        return metrics_file

def correct_coordinates_and_distance(df):
    """
    Corrige les coordonnées et distances pour toutes les lignes du DataFrame.
    """
    logger.info("Correction des coordonnées et distances...")
    
    # Garder une trace des modifications
    corrections = {'coordonnées': 0, 'distances': 0}
    
    # Pour chaque ville unique dans le DataFrame
    unique_cities = df['destination_city'].unique()
    for city in unique_cities:
        old_coords = df.loc[df['destination_city'] == city, 'destination_coordinates'].iloc[0]
        new_coords = format_coordinates(get_city_coordinates(city))
        
        if old_coords != new_coords:
            logger.info(f"Correction pour {city}: {old_coords} -> {new_coords}")
            corrections['coordonnées'] += 1
            
            # Mise à jour des coordonnées
            df.loc[df['destination_city'] == city, 'destination_coordinates'] = new_coords
            
            # Recalcul de la distance
            if new_coords:
                new_coords_tuple = tuple(map(float, new_coords.split(', ')))
                new_distance = round(geodesic(BORDEAUX_COORDS, new_coords_tuple).kilometers, 2)
                old_distance = df.loc[df['destination_city'] == city, 'distance_km'].iloc[0]
                
                if old_distance != new_distance:
                    corrections['distances'] += 1
                    df.loc[df['destination_city'] == city, 'distance_km'] = new_distance
                    logger.info(f"Distance corrigée pour {city}: {old_distance} -> {new_distance} km")
    
    logger.info(f"Corrections effectuées: {corrections['coordonnées']} coordonnées et {corrections['distances']} distances")
    return df

