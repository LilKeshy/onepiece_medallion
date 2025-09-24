import pandas as pd
import os

def load_data():
    input_file = "datalake/silver/characters_clean.csv"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"{input_file} não encontrado. Rode o transform primeiro.")

    df = pd.read_csv(input_file)

    os.makedirs("datalake/gold", exist_ok=True)

    outputs = []

    # 1) Média de bounty por crew
    if "crew" in df.columns and "bounty" in df.columns:
        crew_avg = df.groupby("crew", as_index=False)["bounty"].mean().rename(columns={"bounty":"avg_bounty"})
        out1 = "datalake/gold/crew_avg_bounty.csv"
        crew_avg.to_csv(out1, index=False)
        outputs.append(out1)

    # 2) Contagem por status (vivo/morto/etc)
    if "status" in df.columns:
        status_count = df["status"].value_counts(dropna=False).reset_index()
        status_count.columns = ["status", "count"]
        out2 = "datalake/gold/status_count.csv"
        status_count.to_csv(out2, index=False)
        outputs.append(out2)

    print(f"✅ Gold salvo: {', '.join(outputs) if outputs else 'nenhuma agregação gerada'}")
    return outputs

if __name__ == "__main__":
    load_data()
