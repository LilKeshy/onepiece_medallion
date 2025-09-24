from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# caminhos
SILVER_PATH = "/home/keshy/onepiece_pipeline/onepiece_medallion/datalake/silver"
GOLD_PATH = "/home/keshy/onepiece_pipeline/onepiece_medallion/datalake/gold"
# se quiser usar /datalake direto, troque por:
# GOLD_PATH = "/datalake/gold"

def generate_gold():
    os.makedirs(GOLD_PATH, exist_ok=True)

    # pega o último arquivo CSV da Silver
    files = [f for f in os.listdir(SILVER_PATH) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo encontrado na Silver.")

    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(SILVER_PATH, x)))
    filepath = os.path.join(SILVER_PATH, latest_file)

    # lê dados da Silver
    df = pd.read_csv(filepath)

    # === 1. GOLD FLAT ===
    df_flat = df.copy()

    # recompensa legível (berries)
    def format_bounty(value):
        try:
            if value == 0:
                return "Não tem recompensa"
            elif value >= 1_000_000_000:
                return f"{value/1_000_000_000:.0f} bilhão(ões) de berries"
            elif value >= 1_000_000:
                return f"{value/1_000_000:.0f} milhão(ões) de berries"
            else:
                return f"{value} berries"
        except:
            return "Não tem recompensa"

    df_flat["bounty_human_readable"] = df_flat["bounty"].apply(format_bounty)

    # categoria de recompensa
    def bounty_category(value):
        if value == 0:
            return "Sem recompensa"
        elif value >= 1_000_000_000:
            return "Bilhões"
        elif value >= 1_000_000:
            return "Milhões"
        else:
            return "Outros"

    df_flat["bounty_category"] = df_flat["bounty"].apply(bounty_category)

    # mantém imagem da fruta se existir
    if "fruit_image" in df_flat.columns:
        df_flat["fruit_image"] = df_flat["fruit_image"]
    else:
        df_flat["fruit_image"] = "Não disponível"

    # salva flat
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    flat_csv = os.path.join(GOLD_PATH, f"onepiece_gold_flat_{ts}.csv")
    df_flat.to_csv(flat_csv, index=False)

    # === 2. GOLD ANALYTICS ===
    analytics = {}

    # total de recompensas por tripulação
    crew_rewards = df_flat.groupby("crew")["bounty"].sum().reset_index().sort_values(by="bounty", ascending=False)
    analytics["crew_rewards"] = crew_rewards

    # média de recompensas dos Yonkou
    yonkou_avg = df_flat[df_flat["emperor"] == "Sim"]["bounty"].mean()
    analytics["yonkou_avg"] = pd.DataFrame({"metric": ["Média recompensas Yonkou"], "value": [yonkou_avg]})

    # top 10 recompensas individuais
    top_10 = df_flat.sort_values(by="bounty", ascending=False).head(10)
    analytics["top_10"] = top_10[["name", "bounty", "bounty_human_readable", "crew", "fruit_image"]]

    # estatísticas de frutas
    fruit_stats = df_flat.groupby("fruit").size().reset_index(name="count").sort_values(by="count", ascending=False)
    analytics["fruit_stats"] = fruit_stats

    # status (vivos/mortos)
    status_stats = df_flat.groupby("status").size().reset_index(name="count")
    analytics["status_stats"] = status_stats

    # salva analytics (um único Excel com abas)
    analytics_path = os.path.join(GOLD_PATH, f"onepiece_gold_analytics_{ts}.xlsx")
    with pd.ExcelWriter(analytics_path) as writer:
        for sheet, data in analytics.items():
            data.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ Gold gerado: {flat_csv} e {analytics_path}")


# configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "onepiece_gold_dag",
    default_args=default_args,
    schedule_interval=None,  # só dispara após Silver
    catchup=False,
    tags=["onepiece", "gold"],
) as dag:

    gold_task = PythonOperator(
        task_id="generate_gold",
        python_callable=generate_gold,
    )

    gold_task
