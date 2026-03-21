import pandas as pd
from sqlalchemy import create_engine

def extract_from_sql():
    print("Extraction des données SQL (Postgres)...")
    # Connexion à la base de données définie dans docker-compose
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    try:
        # Simulation d'une table 'crm_customers'
        query = "SELECT * FROM cm_customers" 
        # Note : On va créer cette table juste après
        df = pd.read_sql(query, engine)
        
        file_path = "/opt/airflow/scripts/sql_marketing_data.csv"
        df.to_csv(file_path, index=False)
        print(f"Extraction SQL réussie : {len(df)} clients récupérés.")
    except Exception as e:
        print(f"Erreur SQL (Normal si table pas encore créée) : {e}")

if __name__ == "__main__":
    extract_from_sql()
