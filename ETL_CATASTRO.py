# sri_ruc_etl_dag.py

import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings
from datetime import datetime
from urllib.parse import quote

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator
)

# --- CONFIGURACIÓN ---
# ¡IMPORTANTE! Reemplaza estos valores con los de tu proyecto.
GCP_PROJECT_ID = "docker-ariflow"  # Ejemplo: 'data-sri'
BIGQUERY_DATASET = "sri_data"        # El dataset que creaste en BigQuery
STAGING_TABLE_NAME = "ruc_catastro_staging"
FINAL_TABLE_NAME = "ruc_catastro"
GCP_CONN_ID = "google_cloud_bigquery" # El ID de tu conexión en Airflow

# --- LÓGICA DE TRANSFORMACIÓN (adaptada de tu script) ---
# Esta función se aplicará a cada lote (DataFrame pequeño)
def transformar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica toda la limpieza y transformación a un DataFrame de RUC."""
    
    # 1. Renombrar columnas
    column_mapping = {
        'NUMERO_RUC': 'ruc', 'RAZON_SOCIAL': 'razon_social', 'PROVINCIA_JURISDICCION': 'provincia_jurisdiccion',
        'ESTADO_CONTRIBUYENTE': 'estado_contribuyente', 'CLASE_CONTRIBUYENTE': 'clase_contribuyente',
        'FECHA_INICIO_ACTIVIDADES': 'fecha_inicio_actividades', 'FECHA_ACTUALIZACION': 'fecha_actualizacion',
        'FECHA_SUSPENSION_DEFINITIVA': 'fecha_suspension_definitiva', 'FECHA_REINICIO_ACTIVIDADES': 'fecha_reinicio_actividades',
        'OBLIGADO': 'obligado_contabilidad', 'TIPO_CONTRIBUYENTE': 'tipo_contribuyente',
        'NUMERO_ESTABLECIMIENTO': 'numero_establecimientos', 'NOMBRE_FANTASIA_COMERCIAL': 'nombre_comercial',
        'ESTADO_ESTABLECIMIENTO': 'estado_establecimiento', 'DESCRIPCION_PROVINCIA_EST': 'provincia_establecimiento',
        'DESCRIPCION_CANTON_EST': 'canton_establecimiento', 'DESCRIPCION_PARROQUIA_EST': 'parroquia_establecimiento',
        'CODIGO_CIIU': 'codigo_ciiu', 'ACTIVIDAD_ECONOMICA': 'actividad_economica',
        'AGENTE_RETENCION': 'es_agente_retencion', 'ESPECIAL': 'es_contribuyente_especial'
    }
    # Solo renombramos las columnas que existen para evitar errores
    df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

    # 2. Optimización de Tipos de Datos
    binary_cols = ['obligado_contabilidad', 'es_agente_retencion', 'es_contribuyente_especial']
    for col in df.columns:
        if col in binary_cols:
            # Mapear 'S' a 1, y cualquier otra cosa ('N', nulos) a 0.
            df[col] = (df[col].astype(str).str.strip().str.upper() == 'S').astype('int8')
        elif df[col].dtype == 'object':
            df[col] = df[col].fillna('NO ESPECIFICADO').astype(str).str.strip().str.upper()
            # La conversión a 'category' es mejor hacerla al final en BigQuery si es necesario.
            # Para la carga, el tipo 'string' es más seguro.

    # 3. Conversión de fechas
    date_cols = [col for col in df.columns if 'fecha' in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', format='%d/%m/%Y', dayfirst=True)

    return df


# --- DEFINICIÓN DEL DAG ---
@dag(
    dag_id="sri_ruc_etl_a_BIGQUERY",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",  # Se puede ejecutar mensualmente, semanalmente o None para manual
    catchup=False,
    tags=['sri', 'etl', 'bigquery'],
    description="ETL para cargar el catastro RUC del SRI a BigQuery de forma eficiente en memoria.",
)
def sri_ruc_etl_dag():

    # Reemplaza la función en tu archivo DAG

    @task
    def obtener_links_de_descarga() -> list:
        """
        Escanea la página del SRI y extrae los links de descarga de los CSV del RUC.
        Falla explícitamente si no encuentra ningún link.
        """
        sri_url = 'https://www.sri.gob.ec/datasets'
        print(f"Accediendo a: {sri_url}")
        
        try:
            # Usar cabeceras para simular un navegador puede ayudar a evitar bloqueos
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
            # Aumentar el timeout
            response = requests.get(sri_url, headers=headers, timeout=60)
            # Esto lanzará un error si la página devuelve 404, 500, etc.
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            base_url = "https://descargas.sri.gob.ec"
            download_links = []

            # Un método de búsqueda más robusto: buscar cualquier enlace 'a'
            # y luego filtrar por el contenido del 'href'
            for link in soup.find_all('a'):
                href = link.get('href', '') # Usar .get() para evitar errores si no hay href
                if 'download/datosAbiertos/SRI_RUC_' in href:
                    full_link = href
                    if not full_link.startswith('http'):
                        full_link = base_url + full_link
                    download_links.append(full_link)

            download_links = sorted(list(set(download_links)))

            # --- ESTA ES LA PARTE MÁS IMPORTANTE ---
            # Si después de buscar, la lista de links está vacía, debemos fallar.
            if not download_links:
                print("ERROR: No se encontraron links de descarga. La estructura de la página del SRI pudo haber cambiado.")
                raise ValueError("La lista de links de descarga está vacía.")
            
            print(f"¡Éxito! Se encontraron {len(download_links)} links de descarga:")
            for l in download_links:
                print(f" - {l}")
            
            return download_links

        except requests.exceptions.RequestException as e:
            print(f"Error de red al intentar acceder a la página del SRI: {e}")
            raise # Re-lanzar la excepción para que Airflow marque la tarea como fallida
  
@task
def procesar_y_cargar_lote(link: str):
    """
    Descarga, transforma y carga un solo archivo CSV a la tabla de staging en BigQuery.
    Esta tarea se ejecutará en paralelo para cada link.
    """
    provincia = link.split('_')[-1].replace('.csv', '')
    
    # --- LA SOLUCIÓN ESTÁ AQUÍ ---
    # Codificamos la URL para manejar caracteres especiales como 'ñ' en 'Cañar'
    # El parámetro 'safe' evita que se codifiquen los caracteres ':' y '/'
    safe_link = quote(link, safe=':/')
    # ---------------------------

    print(f"Procesando: {provincia} desde {safe_link}")
    
    try:
        # Usamos la URL segura (safe_link) en lugar de la original
        df_provincia = pd.read_csv(safe_link, sep='|', encoding='latin1', low_memory=False)
        
        # Aplicar la función de transformación
        df_limpio = transformar_dataframe(df_provincia)
        
        print(f"Transformación completa para {provincia}. Cargando {len(df_limpio)} filas a BigQuery Staging...")
        
        # Cargar a la tabla de staging en BigQuery, añadiendo los datos
        df_limpio.to_gbq(
            destination_table=f"{BIGQUERY_DATASET}.{STAGING_TABLE_NAME}",
            project_id=GCP_PROJECT_ID,
            if_exists='append',
            credentials=None,
            chunksize=10000
        )
        print(f"Carga exitosa para {provincia}.")
    except Exception as e:
        # El print ahora mostrará el error real de forma más clara si algo más falla
        print(f"ERROR procesando {provincia} desde {safe_link}: {e}")
        raise e

    # Tarea para consolidar los datos de la tabla staging a la tabla final
    crear_tabla_final_consolidada = BigQueryInsertJobOperator(
        task_id="crear_tabla_final_consolidada",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{FINAL_TABLE_NAME}` AS
                    SELECT *
                    FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{STAGING_TABLE_NAME}`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Tarea para limpiar (borrar) la tabla de staging
    eliminar_tabla_staging = BigQueryDeleteTableOperator(
        task_id="eliminar_tabla_staging",
        gcp_conn_id=GCP_CONN_ID,
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{STAGING_TABLE_NAME}",
        ignore_if_missing=True, # No fallar si la tabla no existe
    )

    # --- Definición del Flujo de Tareas ---
    links = obtener_links_de_descarga()
    
    # La tarea `procesar_y_cargar_lote` se expande para ejecutarse una vez por cada link.
    # Airflow ejecutará estas tareas en paralelo hasta el límite de concurrencia.
    tarea_de_carga = procesar_y_cargar_lote.expand(link=links)
    
    # La consolidación solo ocurre después de que TODAS las cargas de lotes hayan terminado.
    # La limpieza ocurre después de que la consolidación sea exitosa.
    tarea_de_carga >> crear_tabla_final_consolidada >> eliminar_tabla_staging


# Instanciar el DAG
sri_etl = sri_ruc_etl_dag()