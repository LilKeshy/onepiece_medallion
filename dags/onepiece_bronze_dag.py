from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import json
import os

# Caminho Bronze
BRONZE_PATH = "/home/keshy/onepiece_pipeline/onepiece_medallion/datalake/bronze"

def extract_characters():
    url = "https://api.api-onepiece.com/v2/characters/en"
    response = requests.get(url)
    data = response.json()

    os.makedirs(BRONZE_PATH, exist_ok=True)
    filepath = os.path.join(BRONZE_PATH, f"characters_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"✅ Dados salvos em {filepath}")

with DAG(
    dag_id="onepiece_bronze_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # roda 1x por dia
    catchup=False,
    tags=["onepiece", "bronze"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_characters",
        python_callable=extract_characters,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="onepiece_silver_dag",  # ID da DAG Silver
        wait_for_completion=False,
    )

    extract_task >> trigger_silver
