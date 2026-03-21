from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajout du chemin pour que Airflow trouve tes scripts
sys.path.append('/opt/airflow/scripts')

# Import de tes fonctions
from extract_web import scrape_marketing_data
from extract_kaggle import download_github_data
from extract_sql import extract_from_sql
from load_to_minio import upload_to_minio
from transform_silver import transform_to_silver

default_args = {
    'owner': 'david',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_marketing_industrialise',
    default_args=default_args,
    description='Pipeline ETL complet de David (Bronze to Silver)',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_web', python_callable=scrape_marketing_data)
    t2 = PythonOperator(task_id='extract_github', python_callable=download_github_data)
    t3 = PythonOperator(task_id='extract_sql', python_callable=extract_from_sql)
    t4 = PythonOperator(task_id='load_to_bronze', python_callable=upload_to_minio)
    t5 = PythonOperator(task_id='transform_to_silver', python_callable=transform_to_silver)

    # L'ordre d'exécution
    [t1, t2, t3] >> t4 >> t5
