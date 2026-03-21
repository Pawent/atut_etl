import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

def scrape_marketing_data():
    print("Début du scraping Wikipedia (Marketing)...")
    url = "https://en.wikipedia.org/wiki/List_of_advertising_agencies"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # On récupère les noms des agences dans les listes
        agencies = []
        for li in soup.select('div.mw-parser-output ul li'):
            name = li.get_text().split(' – ')[0] # Nettoyage basique
            if len(name) < 50 and len(name) > 2:
                agencies.append({
                    "agence_nom": name,
                    "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "Wikipedia"
                })
        
        df = pd.DataFrame(agencies).head(50) # On en garde 50
        
        file_path = "/opt/airflow/scripts/web_marketing_data.csv"
        df.to_csv(file_path, index=False)
        print(f"Extraction réussie : {len(df)} agences trouvées.")
        
    except Exception as e:
        print(f"Erreur lors du scraping : {e}")

if __name__ == "__main__":
    scrape_marketing_data()
