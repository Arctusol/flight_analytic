import pandas as pd
from pathlib import Path

def count_lines_in_folder(folder_path: str) -> dict:
    """
    Compte le nombre de lignes dans chaque fichier CSV du dossier spécifié
    
    Args:
        folder_path (str): Chemin vers le dossier contenant les fichiers CSV
        
    Returns:
        dict: Dictionnaire avec les noms de fichiers et leur nombre de lignes
    """
    folder = Path(folder_path)
    results = {}
    total_lines = 0
    
    # Vérifier si le dossier existe
    if not folder.exists():
        raise FileNotFoundError(f"Le dossier {folder_path} n'existe pas")
    
    # Parcourir tous les fichiers CSV dans le dossier
    for file in folder.glob('*.csv'):
        try:
            # Lire le fichier CSV
            df = pd.read_csv(file)
            num_lines = len(df)
            results[file.name] = num_lines
            total_lines += num_lines
        except Exception as e:
            print(f"Erreur lors de la lecture de {file.name}: {e}")
    
    # Ajouter le total
    results['TOTAL'] = total_lines
    
    return results

def main():
    # Chemin vers votre dossier combined_old
    folder_path = "data/combined"
    
    try:
        results = count_lines_in_folder(folder_path)
        
        # Afficher les résultats
        print("\nNombre de lignes par fichier:")
        print("-" * 40)
        for filename, count in results.items():
            if filename == 'TOTAL':
                print("-" * 40)
                print(f"TOTAL: {count:,} lignes")
            else:
                print(f"{filename}: {count:,} lignes")
                
    except Exception as e:
        print(f"Une erreur est survenue: {e}")

if __name__ == "__main__":
    main()