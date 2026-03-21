import pandas as pd
import boto3
import io
from datetime import datetime

def transform_to_silver():
    print("Début de la transformation Silver...")
    s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                      aws_access_key_id='admin', aws_secret_access_key='password123')
    
    today = datetime.now().strftime('%Y-%m-%d')
    bucket_bronze = 'bronze'
    bucket_silver = 'silver'
    
    # Créer le bucket silver s'il n'existe pas
    try:
        s3.create_bucket(Bucket=bucket_silver)
        print(f"Bucket '{bucket_silver}' prêt.")
    except:
        pass

    files = ['web_marketing_data.csv', 'kaggle_marketing_data.csv', 'sql_marketing_data.csv']
    
    for f in files:
        try:
            # 1. Lecture depuis MinIO (Bronze)
            print(f"Lecture de {f} depuis Bronze...")
            obj = s3.get_object(Bucket=bucket_bronze, Key=f"{today}/{f}")
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            
            # 2. Transformation (Nettoyage)
            df.columns = [c.lower() for c in df.columns] # Colonnes en minuscules
            df = df.drop_duplicates()                   # Suppression doublons
            df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 3. Écriture vers MinIO (Silver)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_silver, Key=f"{today}/{f}", Body=csv_buffer.getvalue())
            
            print(f"Fichier {f} nettoyé et envoyé vers le bucket Silver.")
            
        except Exception as e:
            print(f"Erreur sur {f} : {e}")

if __name__ == "__main__":
    transform_to_silver()
