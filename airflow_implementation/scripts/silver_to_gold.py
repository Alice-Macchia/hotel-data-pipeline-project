import pandas as pd
import numpy as np
import io
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# FUNZIONI PER IL CALCOLO DEI KPI IN PANDAS

def calculate_daily_revenue(df_bookings):
    """ KPI 1: Calcola il fatturato giornaliero. """
    print(" -> KPI 1: Daily Revenue")
    df_confirmed = df_bookings[df_bookings['status'] == 'confirmed'].copy()
    # Assicura che le date siano solo date, senza ore
    df_confirmed['date'] = pd.to_datetime(df_confirmed['checkin_date']).dt.date
    
    kpi = df_confirmed.groupby('date').agg(
        gross_revenue=('total_amount', 'sum'),
        bookings_count=('booking_id', 'count')
    ).reset_index().sort_values('date')
    return kpi

def calculate_cancellation_rate(df_bookings):
    """ KPI 2: Calcola il tasso di cancellazione per sorgente. """
    print(" -> KPI 2: Cancellation Rate by Source")
    kpi = df_bookings.groupby('source').agg(
        total_bookings=('booking_id', 'count'),
        cancelled=('status', lambda s: (s == 'cancelled').sum())
    ).reset_index()
    
    kpi['cancellation_rate_pct'] = np.where(
        kpi['total_bookings'] > 0,
        (kpi['cancelled'] / kpi['total_bookings']) * 100,
        0
    ).round(2)
    return kpi

def calculate_collection_rate(df_bookings, df_payments):
    """ KPI 3: Calcola il tasso di incasso per hotel. """
    print(" -> KPI 3: Collection Rate by Hotel")
    bookings_value = df_bookings.groupby('hotel_id')['total_amount'].sum().reset_index(name='total_bookings_value')
    
    payments_with_hotel = pd.merge(df_payments, df_bookings[['booking_id', 'hotel_id']], on='booking_id', how='inner')
    payments_value = payments_with_hotel.groupby('hotel_id')['amount'].sum().reset_index(name='total_payments_value')
    
    kpi = pd.merge(bookings_value, payments_value, on='hotel_id', how='left').fillna(0)
    kpi['collection_rate'] = np.where(
        kpi['total_bookings_value'] > 0,
        kpi['total_payments_value'] / kpi['total_bookings_value'],
        0
    )
    return kpi

def calculate_overbooking_alerts(df_bookings):
    """ KPI 4: Identifica le prenotazioni sovrapposte per la stessa stanza. """
    print(" -> KPI 4: Overbooking Alerts")
    # Eseguiamo un self-merge per confrontare ogni prenotazione con ogni altra per la stessa stanza
    b_merged = pd.merge(df_bookings, df_bookings, on='room_id', suffixes=('_1', '_2'))
    
    # Applichiamo le condizioni per trovare le sovrapposizioni reali
    overlap_filter = (
        (b_merged['booking_id_1'] < b_merged['booking_id_2']) &
        (b_merged['checkin_date_1'] < b_merged['checkout_date_2']) &
        (b_merged['checkout_date_1'] > b_merged['checkin_date_2'])
    )
    
    df_overlaps = b_merged[overlap_filter].copy()
    
    # Calcoliamo le date di inizio e fine della sovrapposizione
    df_overlaps['overlap_start'] = df_overlaps[['checkin_date_1', 'checkin_date_2']].max(axis=1)
    df_overlaps['overlap_end'] = df_overlaps[['checkout_date_1', 'checkout_date_2']].min(axis=1)
    
    kpi = df_overlaps[['room_id', 'booking_id_1', 'booking_id_2', 'overlap_start', 'overlap_end']]
    return kpi

def calculate_customer_value(df_bookings, df_customers):
    """ KPI 5: Calcola il valore per cliente. """
    print(" -> KPI 5: Customer Value")
    merged_df = pd.merge(df_bookings, df_customers, on='customer_id')
    
    kpi = merged_df.groupby(['customer_id', 'first_name', 'last_name', 'email']).agg(
        bookings_count=('booking_id', 'count'),
        revenue_sum=('total_amount', 'sum')
    ).reset_index()
    
    kpi['avg_ticket'] = np.where(
        kpi['bookings_count'] > 0,
        kpi['revenue_sum'] / kpi['bookings_count'],
        0
    )
    return kpi.sort_values('revenue_sum', ascending=False)


# FUNZIONE PRINCIPALE PER AIRFLOW (VERSIONE PANDAS)

def run_gold_kpi_generation_task_pandas():
    print("Avvio task di generazione della GOLD LAYER con Pandas...")

    # 1. Connessione ad Azure
    hook = WasbHook(wasb_conn_id='azure_storage_connection')
    client = hook.get_conn()
    CONTAINER_NAME = "datalake"
    
    # 2. Lettura di tutti i dati dalla Silver Layer
    silver_base_path = "silver"
    tables_to_read = ['bookings', 'payments', 'customers', 'rooms']
    dataframes_silver = {}
    
    for table in tables_to_read:
        print(f"Lettura di '{table}' dalla Silver Layer...")
        blob_name = f"{silver_base_path}/{table}/{table}.csv"
        blob_downloader = client.get_blob_client(CONTAINER_NAME, blob_name).download_blob()
        stream = io.BytesIO(blob_downloader.readall())
        dataframes_silver[table] = pd.read_csv(stream)

    # 3. Calcolo di tutti i KPI
    print("\nCalcolo dei KPI...")
    kpi_map = {
        "daily_revenue": calculate_daily_revenue(dataframes_silver['bookings']),
        "cancellation_rate_by_source": calculate_cancellation_rate(dataframes_silver['bookings']),
        "collection_rate_by_hotel": calculate_collection_rate(dataframes_silver['bookings'], dataframes_silver['payments']),
        "overbooking_alerts": calculate_overbooking_alerts(dataframes_silver['bookings']),
        "customer_value": calculate_customer_value(dataframes_silver['bookings'], dataframes_silver['customers']),
    }

    # 4. Scrittura dei KPI nella Gold Layer
    print("\nScrittura dei KPI nella Gold Layer...")
    gold_base_path = "gold"
    for name, df_kpi in kpi_map.items():
        if not df_kpi.empty:
            print(f" -> Salvando {name}...")
            output_buffer = io.BytesIO()
            output_buffer.write(df_kpi.to_csv(index=False, encoding='utf-8').encode('utf-8'))
            output_buffer.seek(0)
            
            gold_blob_name = f"{gold_base_path}/{name}/{name}.csv"
            blob_client = client.get_blob_client(container=CONTAINER_NAME, blob=gold_blob_name)
            blob_client.upload_blob(output_buffer, overwrite=True)

    print("\nGenerazione della GOLD LAYER completata con successo.")
