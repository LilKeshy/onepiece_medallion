import pandas as pd
import json
import os
from glob import glob

def transform_data():
    bronze_files = sorted(glob("datalake/bronze/*.json"))
    if not bronze_files:
        raise FileNotFoundError("Nenhum arquivo encontrado em datalake/bronze")
    latest_file = bronze_files[-1]

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # Seleciona colunas comuns da API (se existir)
    cols = ["id", "name", "bounty", "job", "status", "crew", "devilFruit"]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        # se a API mudar o esquema, pelo menos salva tudo
        cols = df.columns.tolist()

    df = df[cols]

    # Tipagem básica
    if "bounty" in df.columns:
        df["bounty"] = pd.to_numeric(df["bounty"], errors="coerce")

    os.makedirs("datalake/silver", exist_ok=True)
    output_file = "datalake/silver/characters_clean.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ Silver salvo em {output_file}")
    return output_file

if __name__ == "__main__":
    transform_data()
