import logging
from pathlib import Path
from datetime import datetime
import requests
import msal
import yaml
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

class PowerBIAutomation:
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialise l'automatisation Power BI
        
        Args:
            config_path: Chemin vers le fichier de configuration YAML
        """
        self.logger = self._setup_logger()
        self.config = self._load_config(config_path)
        self.access_token = None
        
        # Configuration Azure AD
        self.tenant_id = self.config['powerbi']['tenant_id']
        self.client_id = self.config['powerbi']['client_id']
        self.client_secret = self.config['powerbi']['client_secret']
        self.workspace_id = self.config['powerbi']['workspace_id']
        
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
        
        # Configuration BigQuery
        self.project_id = self.config['bigquery']['id']
        self.dataset = self.config['bigquery']['dataset']
        self.credentials_path = self.config['bigquery']['credentials_path']
    
    def _setup_logger(self) -> logging.Logger:
        """Configure le système de logging"""
        logger = logging.getLogger('powerbi_automation')
        logger.setLevel(logging.INFO)
        
        # Handler pour console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Format du log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        return logger
    
    def _load_config(self, config_path: str) -> dict:
        """Charge la configuration depuis le fichier YAML"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def authenticate(self) -> None:
        """Authentification via MSAL pour obtenir un token"""
        try:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=authority
            )
            
            scopes = ["https://analysis.windows.net/powerbi/api/.default"]
            result = app.acquire_token_silent(scopes, account=None)
            
            if not result:
                result = app.acquire_token_for_client(scopes=scopes)
            
            if "access_token" in result:
                self.access_token = result["access_token"]
                self.logger.info("Authentification Power BI réussie")
            else:
                self.logger.error(f"Erreur d'authentification: {result.get('error')}")
                raise Exception("Échec de l'authentification Power BI")
                
        except Exception as e:
            self.logger.error(f"Erreur lors de l'authentification: {e}")
            raise
    
    def refresh_dataset(self, dataset_id: str) -> None:
        """
        Rafraîchit un dataset spécifique
        
        Args:
            dataset_id: ID du dataset à rafraîchir
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            url = f"{self.base_url}/datasets/{dataset_id}/refreshes"
            response = requests.post(url, headers=headers)
            
            if response.status_code == 202:
                self.logger.info(f"Rafraîchissement du dataset {dataset_id} initié")
            else:
                self.logger.error(f"Erreur lors du rafraîchissement: {response.text}")
                response.raise_for_status()
                
        except Exception as e:
            self.logger.error(f"Erreur lors du rafraîchissement du dataset: {e}")
            raise
    
    def get_refresh_status(self, dataset_id: str) -> Optional[str]:
        """
        Vérifie le statut du dernier rafraîchissement
        
        Args:
            dataset_id: ID du dataset
        Returns:
            Le statut du rafraîchissement ou None en cas d'erreur
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.access_token}",
            }
            
            url = f"{self.base_url}/datasets/{dataset_id}/refreshes?$top=1"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                refreshes = response.json().get('value', [])
                if refreshes:
                    return refreshes[0].get('status')
            return None
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la vérification du statut: {e}")
            return None
    
    def setup_bigquery_client(self) -> bigquery.Client:
        """Configure le client BigQuery"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
        except Exception as e:
            self.logger.error(f"Erreur lors de la configuration BigQuery: {e}")
            raise
    
    def get_bigquery_data(self, query: str) -> pd.DataFrame:
        """
        Récupère les données de BigQuery
        
        Args:
            query: Requête SQL à exécuter
        Returns:
            DataFrame contenant les résultats
        """
        try:
            client = self.setup_bigquery_client()
            self.logger.info("Exécution de la requête BigQuery")
            
            df = client.query(query).to_dataframe()
            self.logger.info(f"Données récupérées avec succès: {len(df)} lignes")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des données: {e}")
            raise
    
    def upload_dataframe_to_powerbi(self, df: pd.DataFrame, dataset_id: str, table_name: str) -> None:
        """
        Charge un DataFrame dans Power BI
        
        Args:
            df: DataFrame à charger
            dataset_id: ID du dataset Power BI
            table_name: Nom de la table de destination
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            url = f"{self.base_url}/datasets/{dataset_id}/tables/{table_name}/rows"
            
            # Conversion du DataFrame en format JSON accepté par Power BI
            data = df.to_dict('records')
            
            response = requests.post(url, headers=headers, json={'rows': data})
            
            if response.status_code in [200, 202]:
                self.logger.info(f"Données chargées avec succès dans la table {table_name}")
            else:
                self.logger.error(f"Erreur lors du chargement: {response.text}")
                response.raise_for_status()
                
        except Exception as e:
            self.logger.error(f"Erreur lors du chargement des données: {e}")
            raise
    
    def get_flight_data(self) -> pd.DataFrame:
        """
        Récupère les données de vol depuis le dataset flight_data
        
        Returns:
            DataFrame contenant les données de vol
        """
        try:
            client = self.setup_bigquery_client()
            self.logger.info(f"Récupération des données depuis {self.dataset}")
            
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset}.flights`
            ORDER BY flight_date DESC
            """
            
            df = client.query(query).to_dataframe()
            self.logger.info(f"Données de vol récupérées avec succès: {len(df)} lignes")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des données de vol: {e}")
            raise

def main():
    try:
        # Initialisation
        pbi = PowerBIAutomation()
        
        # Authentification
        pbi.authenticate()
        
        # Récupération des données de vol
        df = pbi.get_flight_data()
        
        # Chargement dans Power BI
        dataset_id = "votre_dataset_id"
        table_name = "flights"
        pbi.upload_dataframe_to_powerbi(df, dataset_id, table_name)
        
        # Rafraîchissement du dataset
        pbi.refresh_dataset(dataset_id)
        
    except Exception as e:
        print(f"Erreur dans le programme principal: {e}")

if __name__ == "__main__":
    main() 