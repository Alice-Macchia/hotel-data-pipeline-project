# 🏨 GlobalStay Hotel Analytics: Pipeline ETL & ML su Databricks

[![Azure Resource Group](https://img.shields.io/badge/Azure%20Resource%20Group-0078D4?logo=microsoftazure)](https://portal.azure.com/#@laboratorioacademy.onmicrosoft.com/resource/subscriptions/4e24ce49-a5f9-4420-a0f4-368706743360/resourceGroups/rg-project-alice/overview) **| Nome Resource Group:** `rg-project-alice`

Questo progetto implementa una pipeline di dati end-to-end sulla piattaforma Databricks per processare, analizzare e modellare i dati di prenotazione di una catena alberghiera. L'obiettivo è trasformare dati grezzi (CSV) in insight di business pronti per l'analisi, attraverso una dashboard interattiva e un modello di machine learning.

## 🏛️ Architettura della Pipeline

La soluzione adotta il modello architetturale **Medallion** a tre livelli, interamente implementato su Databricks per garantire scalabilità, governance e un ambiente di lavoro unificato.

**Flusso dei Dati:**
`File CSV Sorgente` → `Volume (Unity Catalog)` → `🥉 Bronze Layer` → `🥈 Silver Layer` → `🥇 Gold Layer`

---

## 🚀 Stack Tecnologico

* **Piattaforma Cloud:** Databricks
* **Data Governance:** Unity Catalog
* **Pipeline Framework:** Delta Live Tables (DLT) 
* **Orchestrazione:** Databricks Jobs (Workflows) 
* **Librerie di Trasformazione:** PySpark
* **Librerie ML:** Scikit-learn
* **Reportistica:** Databricks Dashboards

---

## 🛠️ Fasi del Progetto

Il progetto è suddiviso in 5 fasi principali, dalla ricezione del dato grezzo fino alla sua visualizzazione e utilizzo predittivo.

### 1. Ingestion (Bronze Layer)
I dati grezzi dai file CSV vengono caricati "così come sono" in tabelle Delta. Viene aggiunta una colonna `ingestion_date` per tracciare il caricamento. 

### 2. Pulizia e Trasformazione (Silver Layer)
I dati della Bronze Layer vengono puliti, standardizzati e validati per creare una "source of truth" affidabile. Le regole di Data Quality includono:
* **Customers:** Rimozione duplicati e gestione di email nulle. 
* **Bookings:** Correzione di date invertite e importi negativi.
* **Payments:** Identificazione di pagamenti "orfani" o anomali.
  
### 3. KPI di Business (Gold Layer)
Dalla Silver Layer vengono calcolate 5 tabelle aggregate con i KPI fondamentali per il business:
* **Daily Revenue:** Ricavo totale e numero di prenotazioni per giorno.
* **Cancellation Rate by Source:** Percentuale di cancellazioni per canale di prenotazione. 
* **Collection Rate:** Tasso di incasso effettivo per ogni hotel.
* **Overbooking Alerts:** Segnalazione di prenotazioni sovrapposte per la stessa camera. 
* **Customer Value:** Valore di ogni cliente (prenotazioni totali, spesa media e totale). 

### 4. Machine Learning - Previsione Prezzi
Un modello di Machine Learning viene addestrato sui dati puliti della Silver Layer per prevedere il costo di una prenotazione.
* **Obiettivo:** Prevedere la colonna `total_amount`. 
* **Modello:** `Random Forest Regressor` con Scikit-learn. 
* **Performance:** Il modello spiega l'**85%** della variabilità del prezzo (R² = 0.85). 
* **Integrazione:** Le previsioni vengono salvate come colonna `predicted_price` in una nuova tabella Gold, rendendo i risultati accessibili via SQL. 

### 5. Reportistica - Dashboard di Business
I KPI calcolati nella Gold Layer vengono visualizzati in una dashboard interattiva costruita con Databricks Dashboards. La dashboard è organizzata con un layout "top-down":
* **KPI Principali:** Contatori per una visione d'insieme (Fatturato, Tasso d'incasso).
* **Trend e Confronti:** Grafici a linee e a barre per analizzare andamenti e performance.
* **Dettagli Operativi:** Tabelle per consultare dati granulari come alert di overbooking e classifica clienti.

---

## ⚙️ Orchestrazione e Automazione

L'intera pipeline è automatizzata utilizzando un  **Databricks Job (Workflow)** che orchestra l'esecuzione di tre Pipeline Delta Live Tables separate, una per ogni layer (Bronze, Silver, Gold).

Il Job è composto da tre task sequenziali. Ogni task avvia una delle pipeline DLT pre-configurate, e le dipendenze tra i task ("Depends on") garantiscono che l'esecuzione avvenga nell'ordine corretto:

`Task 1: Avvia DLT_Bronze_Pipeline` → `Task 2: Avvia DLT_Silver_Pipeline` → `Task 3: Avvia DLT_Gold_Pipeline`

---

## ▶️ Come Eseguire il Progetto

1.  **Caricamento Dati:** Caricare i 5 file CSV sorgente nel Volume `catalog_progetto_finale.schema_pf.volume_pf` definito in Unity Catalog. 
2.  **Esecuzione Pipeline:** Avviare il job principale `Job_Progetto_Finale` dalla sezione Jobs & Pipelines di Databricks. 
3.  **Consultazione Risultati:** Al termine del job, le tabelle finali saranno disponibili nello schema `gold_schema_pf` e la dashboard mostrerà i dati aggiornati. 

---

## ☁️ Implementazione Alternativa: Apache Airflow e Azure

Oltre all'implementazione su Databricks, è stata sviluppata una versione alternativa della pipeline per dimostrare la capacità di integrazione con servizi cloud esterni. Questa versione utilizza **Apache Airflow** per l'orchestrazione e **Azure Blob Storage** come Data Lake.

Per questa implementazione, gli script di trasformazione sono stati volutamente semplificati utilizzando la libreria **Pandas**, ideale per la manipolazione di dati in memoria su set di dati di dimensioni contenute.

### Architettura e Stack Tecnologico (Airflow)

* **Orchestrazione:** Apache Airflow 
* **Logica di Trasformazione:** Python con Pandas
* **Data Lake (Storage):** Azure Blob Storage 
* **Connessione Cloud:** Airflow `WasbHook` per l'interazione con Azure 

### Funzionamento della Pipeline (Airflow)

L'intera pipeline ETL è gestita da un unico DAG Airflow (`globalstay_etl_dag.py`) che orchestra l'esecuzione sequenziale di tre script Python.

1.  **Task 1: Ingestion (Bronze):** Un `PythonOperator` legge i file CSV sorgente dal container `landing-zone` di Azure, aggiunge la colonna `ingestion_date` e salva i dati in formato CSV nel percorso `datalake/bronze/`. 
2.  **Task 2: Trasformazione (Silver):** Il secondo task parte al successo del primo. Legge i dati dalla Bronze Layer ed esegue tutte le operazioni di pulizia e validazione (gestione NULL, duplicati, date, importi, etc.) usando Pandas, per poi salvare i dati puliti in `datalake/silver/`. 
3.  **Task 3: Aggregazione (Gold):** L'ultimo task legge i dati puliti dalla Silver Layer e calcola i 5 KPI di business, salvando ogni KPI come un file CSV separato in `datalake/gold/`. 

La dipendenza tra i task (`ingest_to_bronze >> bronze_to_silver >> silver_to_gold`) garantisce l'integrità del flusso.

### 📄 Dettagli di Esecuzione del DAG

Per una visione dettagliata dell'esecuzione del DAG in Airflow, comprese le configurazioni della connessione ad Azure, i log dei task e gli screenshot della Web UI, si prega di consultare il file **`Esecuzione_DAG_Airflow.docx`** presente in questa repository.

---

## 📂 Struttura della Repository

La repository è organizzata per separare in modo netto le due diverse implementazioni del progetto, i dati e la documentazione.

```
├── airflow_implementation/     # --- Logica per l'implementazione con Airflow ---
│   ├── dags/                   # Contiene il file del DAG
│   └── scripts/                # Script Python chiamati dal DAG (Bronze, Silver, Gold)
│
├── databricks_implementation/  # --- Logica per l'implementazione con Databricks ---
│   ├── definitions/            # File JSON di configurazione per Job e Dashboard
│   └── notebooks/              # Notebook di Databricks (DLT e ML)
│
├── data/                       # File CSV sorgente utilizzati dalla pipeline
│
├── docs/                       # Documentazione di supporto (es. screenshot esecuzione Airflow)
│
├── .gitignore                  # Specifica i file da ignorare (es. cache, venv)
├── README.md                   # Questo file: la documentazione principale del progetto
└── requirements.txt            # Librerie Python necessarie per l'implementazione Airflow
```
