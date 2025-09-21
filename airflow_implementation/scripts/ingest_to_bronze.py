import pandas as pd
import io
from datetime import datetime
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def run_bronze_ingestion_task():
    #Legge i file CSV dalla landing-zone su Azure, aggiunge la data di ingestione e li salva in formato csv nella Bronze Layer.
 
    print("Avvio task di ingestion da Azure Storage (Landing Zone -> Bronze)...")
    
    hook = WasbHook(wasb_conn_id='azure_storage_connection')
    client = hook.get_conn() # Ottiene il client di connessione

    LANDING_ZONE_CONTAINER = "landing-zone"
    BRONZE_CONTAINER = "datalake"
    
    ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    blob_list = client.get_container_client(LANDING_ZONE_CONTAINER).list_blobs()
    csv_files = [blob.name for blob in blob_list if blob.name.endswith('.csv')]

    print(f"Trovati {len(csv_files)} file CSV da processare: {csv_files}")

    for blob_name in csv_files:
        table_name = blob_name.replace('.csv', '')
        
        print(f"Processing {blob_name}...")
		
		#Il file CSV viene scaricato da Azure non su disco, ma direttamente in un buffer in memoria RAM (stream)
        blob_downloader = client.get_blob_client(LANDING_ZONE_CONTAINER, blob_name).download_blob()
        stream = io.BytesIO(blob_downloader.readall())
        df = pd.read_csv(stream)
        
        df['ingestion_date'] = ingestion_date
        output_buffer = df.to_csv(index=False, encoding='utf-8').encode('utf-8')
        
        # Usa il client per ottenere un puntatore al blob di destinazione
        # e poi usa il suo metodo .upload_blob()
        bronze_blob_name = f"bronze/{table_name}/{table_name}.csv"
        blob_client = client.get_blob_client(
            container=BRONZE_CONTAINER,
            blob=bronze_blob_name
        )
        blob_client.upload_blob(output_buffer, overwrite=True)
        
        print(f" -> {blob_name} caricato con successo in {bronze_blob_name}")

    print("Task di ingestion Bronze completato.")
