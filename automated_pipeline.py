import logging
from datetime import datetime
from pathlib import Path
import subprocess
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import yaml
import json
import pandas as pd
from typing import List, Dict

# Configuration du logging
def setup_logger(base_folder):
    # Créer un dossier pour les logs s'il n'existe pas
    log_folder = base_folder / 'logs'
    log_folder.mkdir(exist_ok=True)
    
    # Créer un nom de fichier avec la date
    log_filename = log_folder / f'automated_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    # Configuration du logger
    logger = logging.getLogger('automated_pipeline')
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

def run_concatenator():
    """Exécute le script concatenator.py"""
    try:
        subprocess.run(['python', 'concatenator.py'], check=True)
        logger.info("Concatenation des fichiers terminée avec succès")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de l'exécution du concatenator: {e}")
        raise

def upload_to_bigquery(project_id, dataset_id, credentials_path):
    """Upload les fichiers CSV vers BigQuery"""
    logger.info(f"Début de l'upload vers BigQuery - Projet: {project_id}, Dataset: {dataset_id}")
    
    # Configuration des credentials
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    # Initialisation du client BigQuery
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    # Chemin vers le dossier contenant les CSV
    base_folder = Path(r"C:\Users\antob\OneDrive - Mac-P'AI\Documents\Arctusol\Scrapping_project\Scraping\data")
    combined_folder = base_folder / 'combined'
    
    # Pour chaque fichier CSV dans le dossier
    for csv_file in combined_folder.glob('*.csv'):
        table_name = csv_file.stem
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        logger.info(f"Traitement du fichier: {csv_file.name}")
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        try:
            with open(csv_file, "rb") as source_file:
                job = client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config
                )
            
            job.result()
            logger.info(f"Chargement terminé pour {table_name}: {job.output_rows} lignes chargées")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {table_name}: {e}")
            raise

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

class FlightDataLoader:
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
    
    def load_single_file(self, file_path: Path) -> pd.DataFrame:
        """Charge un fichier JSON en DataFrame"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Extraction des métadonnées
        metadata = {
            'search_date': data['search_date'],
            'flight_date': data['flight_date'],
            'origin': data['origin'],
            'destination': data['destination']
        }
        
        # Conversion des vols en DataFrame
        df = pd.DataFrame(data['flights'])
        
        # Ajout des métadonnées
        for key, value in metadata.items():
            df[key] = value
            
        return df
    
    def load_destination_data(self, destination: str) -> pd.DataFrame:
        """Charge tous les fichiers d'une destination"""
        destination_path = self.data_dir / destination
        all_files = list(destination_path.glob('*.json'))
        
        dfs = []
        for file_path in all_files:
            df = self.load_single_file(file_path)
            dfs.append(df)
            
        return pd.concat(dfs, ignore_index=True)

def main():
    # Initialisation du logger avec le chemin de base
    base_folder = Path(r"C:\Users\antob\OneDrive - Mac-P'AI\Documents\Arctusol\Scrapping_project\Scraping\data")
    global logger
    logger = setup_logger(base_folder)
    
    try:
        # Chargement de la configuration
        config = load_config()
        
        PROJECT_ID = config['project']['id']
        DATASET_ID = config['project']['dataset']
        CREDENTIALS_PATH = config['project']['credentials_path']
        
        logger.info("Démarrage du pipeline automatisé avec configuration")
        logger.info(f"Configuration chargée: {config}")
        
        # Vérification des credentials
        if not Path(CREDENTIALS_PATH).exists():
            raise FileNotFoundError(f"Le fichier de credentials n'existe pas: {CREDENTIALS_PATH}")
            
        # Exécution du concatenator
        run_concatenator()
        
        # Upload vers BigQuery
        upload_to_bigquery(PROJECT_ID, DATASET_ID, CREDENTIALS_PATH)
        
        logger.info("Pipeline terminé avec succès!")
        
    except FileNotFoundError as e:
        logger.error(f"Erreur de credentials: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur dans le pipeline: {e}")
        raise

if __name__ == "__main__":
    main() 