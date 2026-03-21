import pandas as pd
import requests
import io

def download_github_data():
    # Ton lien Raw GitHub personnel
    url = "https://raw.githubusercontent.com/Pawent/formation_openclassroom/refs/heads/main/kaggle_marketing_data.csv"
    
    print(f"Extraction depuis GitHub : {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Chargement en DataFrame pour validation
        df = pd.read_csv(io.StringIO(response.text))
        
        # Sauvegarde dans le dossier partagé Docker
        file_path = "/opt/airflow/scripts/kaggle_marketing_data.csv"
        df.to_csv(file_path, index=False)
        
        print(f"Succès ! {len(df)} lignes récupérées depuis ton dépôt GitHub.")
        print(f"Colonnes détectées : {list(df.columns)}")
        
    except Exception as e:
        print(f"Erreur lors de l'accès à GitHub : {e}")

if __name__ == "__main__":
    download_github_data()
