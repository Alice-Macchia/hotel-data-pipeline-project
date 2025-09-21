import pandas as pd
import numpy as np
import io
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# LOGICA DI PULIZIA 

def clean_hotels(df):
    print("  -> Pulizia 'hotels': rimozione country 'XX'")
    return df[df['country'] != 'XX'].copy()

def clean_customers(df):
    print("  -> Pulizia 'customers': gestione email nulle e duplicati")
    df['email'] = df['email'].replace(r'^\s*$', np.nan, regex=True)
    return df.drop_duplicates(subset=['customer_id'], keep='first').copy()

def clean_rooms(df):
    print("  -> Pulizia 'rooms': rimozione duplicati")
    return df.drop_duplicates(subset=['room_id'], keep='first').copy()

def clean_bookings(df):
    print("  -> Pulizia 'bookings': correzione date, importi e valute")
    df_clean = df.copy()
    #Converto le colonne di data in un formato standard, gestendo eventuali errori
    df_clean['checkin_date'] = pd.to_datetime(df_clean['checkin_date'], errors='coerce')
    df_clean['checkout_date'] = pd.to_datetime(df_clean['checkout_date'], errors='coerce')
    
    #Identifico e correggo le prenotazioni con date invertite (check-in dopo il check-out)
    inverted_dates_mask = df_clean['checkin_date'] > df_clean['checkout_date']
    temp_checkin = df_clean.loc[inverted_dates_mask, 'checkin_date']
    df_clean.loc[inverted_dates_mask, 'checkin_date'] = df_clean.loc[inverted_dates_mask, 'checkout_date']
    df_clean.loc[inverted_dates_mask, 'checkout_date'] = temp_checkin
    
    df_clean['total_amount'] = df_clean['total_amount'].apply(lambda x: np.nan if x < 0 else x)
    valid_currencies = ['EUR', 'USD', 'GBP']
    df_clean['currency'] = df_clean['currency'].apply(lambda x: x if x in valid_currencies else np.nan)
    return df_clean

def clean_payments(df_payments, df_bookings_clean):
    print("  -> Pulizia 'payments': marcatura anomalie")
    df_clean = df_payments.copy()
    bookings_renamed = df_bookings_clean[['booking_id', 'total_amount']].rename(
        columns={'total_amount': 'total_amount_booking'}
    )
    merged_df = pd.merge(df_clean, bookings_renamed, on='booking_id', how='left')
    df_clean['dq_orphan'] = merged_df['total_amount_booking'].isnull()
    df_clean['dq_over_amount'] = merged_df['amount'] > merged_df['total_amount_booking']
    valid_currencies = ['EUR', 'USD', 'GBP']
    df_clean['currency'] = df_clean['currency'].apply(lambda x: x if x in valid_currencies else np.nan)
    return df_clean

# FUNZIONE PRINCIPALE PER AIRFLOW

def run_silver_transformation_task():
    """
    Legge i dati csv dalla Bronze Layer, applica le regole di pulizia
    e salva i dati trasformati nella Silver Layer.
    """
    print("Avvio task di trasformazione (Bronze -> Silver)...")
    hook = WasbHook(wasb_conn_id='azure_storage_connection')
    client = hook.get_conn()
    
    CONTAINER_NAME = "datalake"
    TABLES = ['hotels', 'customers', 'rooms', 'bookings', 'payments']
    
    # 1. Legge tutti i dati dalla Bronze Layer 
    dataframes_bronze = {}
    for table in TABLES:
        print(f"Lettura di '{table}' dalla Bronze Layer...")
        blob_name = f"bronze/{table}/{table}.csv"
        blob_downloader = client.get_blob_client(CONTAINER_NAME, blob_name).download_blob()
        stream = io.BytesIO(blob_downloader.readall())
        dataframes_bronze[table] = pd.read_csv(stream)
        
    # 2. Applica la logica di pulizia in ordine
    print("Applicazione delle regole di pulizia...")
    hotels_silver = clean_hotels(dataframes_bronze['hotels'])
    customers_silver = clean_customers(dataframes_bronze['customers'])
    rooms_silver = clean_rooms(dataframes_bronze['rooms'])
    bookings_silver = clean_bookings(dataframes_bronze['bookings'])
    payments_silver = clean_payments(dataframes_bronze['payments'], bookings_silver) #chiamo i dati già puliti
    
    silver_data_to_write = {
        'hotels': hotels_silver,
        'customers': customers_silver,
        'rooms': rooms_silver,
        'bookings': bookings_silver,
        'payments': payments_silver
    }
    
    # 3. Scrive i dati puliti nella Silver Layer 
    for table_name, df in silver_data_to_write.items():
        print(f"Scrittura di '{table_name}' nella Silver Layer...")
        
        output_buffer = df.to_csv(index=False, encoding='utf-8').encode('utf-8')
        
        silver_blob_name = f"silver/{table_name}/{table_name}.csv"
        blob_client = client.get_blob_client(
            container=CONTAINER_NAME,
            blob=silver_blob_name
        )
        blob_client.upload_blob(output_buffer, overwrite=True)

    print("Task di trasformazione Silver completato con successo.")
