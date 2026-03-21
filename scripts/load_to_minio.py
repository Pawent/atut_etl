import boto3
import os
from datetime import datetime

def upload_to_minio():
    # Configuration MinIO (depuis ton docker-compose)
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123'
    )
    
    bucket_name = 'bronze'
    
    # Créer le bucket s'il n'existe pas
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' créé.")

    # Liste des fichiers à charger
    files = [
        'web_marketing_data.csv',
        'kaggle_marketing_data.csv',
        'sql_marketing_data.csv'
    ]
    
    # Date du jour pour le partitionnement
    today = datetime.now().strftime('%Y-%m-%d')
    
    for file_name in files:
        local_path = f"/opt/airflow/scripts/{file_name}"
        if os.path.exists(local_path):
            # Chemin S3 partitionné : bronze/2026-03-21/fichier.csv
            s3_path = f"{today}/{file_name}"
            s3.upload_file(local_path, bucket_name, s3_path)
            print(f"Fichier {file_name} chargé dans MinIO : {bucket_name}/{s3_path}")
        else:
            print(f"Erreur : {file_name} introuvable localement.")

if __name__ == "__main__":
    upload_to_minio()
