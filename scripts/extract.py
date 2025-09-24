import requests
import json
import os
from datetime import datetime

API_URL = "https://api.api-onepiece.com/v2/characters/en"

def extract_data():
    resp = requests.get(API_URL, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Erro na API {API_URL}: {resp.status_code} - {resp.text[:200]}")
    data = resp.json()

    os.makedirs("datalake/bronze", exist_ok=True)
    filename = f"datalake/bronze/characters_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ Bronze salvo em {filename}")
    return filename

if __name__ == "__main__":
    extract_data()
