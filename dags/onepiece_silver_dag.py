from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json

# caminhos
BRONZE_PATH = "/home/keshy/onepiece_pipeline/onepiece_medallion/datalake/bronze"
SILVER_PATH = "/home/keshy/onepiece_pipeline/onepiece_medallion/datalake/silver"

def normalize_bronze():
    os.makedirs(SILVER_PATH, exist_ok=True)

    # pega todos os arquivos JSON do bronze
    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".json")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo encontrado no Bronze.")

    data_list = []
    for file in files:
        filepath = os.path.join(BRONZE_PATH, file)
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    data_list.extend(data)
                else:
                    data_list.append(data)
            except json.JSONDecodeError:
                print(f"⚠️ Erro ao ler {file}, ignorando...")

    df = pd.DataFrame(data_list)

    # normalização e schema
    df_silver = pd.DataFrame({
        "id": pd.to_numeric(df.get("id"), errors="coerce").fillna(0).astype(int),
        "name": df.get("name").astype(str),

        # bounty vem como string ("3.000.000.000") -> limpa e converte
        "bounty": df.get("bounty")
            .replace({None: "0"})
            .astype(str)
            .str.replace(r"[^\d]", "", regex=True)
            .replace("", "0")
            .astype(int),

        # nome da tripulação (se existir)
        "crew": df.get("crew").apply(
            lambda x: x.get("name") if isinstance(x, dict) and x.get("name") else "Sem tripulação"
        ),

        # nome da fruta (roman_name quando existir)
        "fruit": df.get("fruit").apply(
            lambda x: x.get("roman_name") if isinstance(x, dict) and x.get("roman_name") else "Não tem"
        ),

        # status traduzido
        "status": df.get("status").replace({
            "alive": "Vivo",
            "deceased": "Morto",
            "vivant": "Vivo",   # francês no JSON
            None: "Desconhecido"
        }).fillna("Desconhecido"),

        # ocupação
        "job": df.get("job", pd.Series(["Outro"] * len(df))).replace({
            "pirate": "Pirata",
            "marine": "Marinha",
            "revolutionary": "Revolucionário",
            None: "Outro"
        }).fillna("Outro"),

        # se é Yonkou ou não
        "emperor": df.get("crew").apply(
            lambda x: "Sim" if isinstance(x, dict) and x.get("is_yonko") else "Não"
        ),

        # imagem da fruta (quando existir)
        "fruit_image": df.get("fruit").apply(
            lambda x: x.get("filename") if isinstance(x, dict) and x.get("filename") else None
        )
    })

    # timestamp para os nomes dos arquivos
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # salva em parquet
    output_parquet = os.path.join(SILVER_PATH, f"onepiece_{ts}.parquet")
    df_silver.to_parquet(output_parquet, index=False)

    # salva também em CSV
    output_csv = os.path.join(SILVER_PATH, f"onepiece_{ts}.csv")
    df_silver.to_csv(output_csv, index=False)

    print(f"✅ Dados normalizados e salvos em {output_parquet} e {output_csv}")


# configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "onepiece_silver_dag",
    default_args=default_args,
    schedule_interval=None,  # só dispara quando a Bronze terminar
    catchup=False,
    tags=["onepiece", "silver"],
) as dag:

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=normalize_bronze,
    )

    bronze_to_silver_task

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    "onepiece_silver_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["onepiece", "silver"],
) as dag:

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=normalize_bronze,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="onepiece_gold_dag",  # ID da DAG Gold
        wait_for_completion=False
    )

    bronze_to_silver_task >> trigger_gold
