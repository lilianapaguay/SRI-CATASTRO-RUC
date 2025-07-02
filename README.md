# SRI-CATASTRO-RUC
# ETL Catastro del RUC 

Este proyecto contiene un flujo de procesamiento ETL para los datos del Catastro del RUC del SRI (Servicio de Rentas Internas de Ecuador).

### 🛠 Tecnologías
- Apache Airflow
- Python (pandas, requests, bs4)
- Google BigQuery
- Google Cloud Platform

### 📂 Contenido
- `dags/`: archivo del DAG (`CATASTRO.py`)
- `Archivos CSV/`: archivos de muestra
- `scripts/`: scripts de apoyo

### 📌 Objetivo
Automatizar la descarga, transformación y carga de datos del SRI a BigQuery para análisis tributario y estadístico.

---
