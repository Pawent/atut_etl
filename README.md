# Pipeline ETL Marketing Industrialisé - David

Ce projet implémente un pipeline de données automatisé pour collecter, stocker et transformer des données marketing provenant de sources multiples.

## 🏗 Architecture (Medallion)
Le projet suit une architecture de Data Lakehouse :
* **Bronze Layer** : Données brutes stockées dans MinIO (S3).
* **Silver Layer** : Données nettoyées et uniformisées avec Pandas.

## 🛠 Technologies utilisées
* **Orchestration** : Apache Airflow
* **Stockage Objet** : MinIO (S3-Compatible)
* **Base de données** : PostgreSQL (Données CRM)
* **Traitement** : Python & Pandas
* **Conteneurisation** : Docker & Docker Compose

## 🚀 Sources de données
1. **Web Scraping** : Récupération des agences leaders via Wikipedia.
2. **Base SQL** : Extraction des segments clients depuis Postgres.
3. **Fichier Externe** : Téléchargement d'un dataset E-commerce depuis GitHub.

## 🔧 Installation et Lancement
1. Lancer les services : `sudo docker compose up -d`
2. Accéder à Airflow : [http://localhost:8080](http://localhost:8080) (admin/admin)
3. Accéder à MinIO : [http://localhost:9001](http://localhost:9001) (admin/password123)

## ✅ Qualité et Tests
* Tests unitaires réalisés avec **Pytest**.
* Couverture de code > 80% sur les fonctions de transformation.
