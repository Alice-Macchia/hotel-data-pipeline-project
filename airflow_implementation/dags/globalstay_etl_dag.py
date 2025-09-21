import sys
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Aggiunge la cartella degli script al path di Python per permettere l'importazione
# Assicurati che questo percorso sia corretto per la tua VM
DAGS_SCRIPTS_PATH = '/home/dataeng/airflow/dags/scripts'
if DAGS_SCRIPTS_PATH not in sys.path:
    sys.path.append(DAGS_SCRIPTS_PATH)

# Importa le funzioni che verranno eseguite dai task
from ingest_to_bronze import run_bronze_ingestion_task
from bronze_to_silver import run_silver_transformation_task
from silver_to_gold import run_gold_kpi_generation_task_pandas

with DAG(
    dag_id='globalstay_full_etl_pipeline',
    start_date=datetime(2023, 1, 1), # È buona norma usare una start_date nel passato
    schedule=None,
    catchup=False,
    tags=['globalstay', 'project', 'azure']
) as dag:
    
    ingest_to_bronze_layer = PythonOperator(
        task_id='ingest_to_bronze_layer',
        python_callable=run_bronze_ingestion_task
    )
    
    transform_to_silver_layer = PythonOperator(
        task_id='transform_to_silver_layer_spark',
        python_callable=run_silver_transformation_task
    )
    
    generate_gold_layer = PythonOperator(
        task_id='aggregate_to_gold_layer_spark',
        python_callable=run_gold_kpi_generation_task_pandas
    )

    # definizione delle dipendenze
    ingest_to_bronze_layer >> transform_to_silver_layer >> generate_gold_layer
