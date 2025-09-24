#!/bin/bash

export AIRFLOW_HOME=/home/keshy/onepiece_pipeline/onepiece_medallion


# Ativar o virtual environment
source .venv/bin/activate

# Resetar DB se estiver quebrado (opcional, comente se não quiser)
# rm -f airflow.db
# airflow db reset -y

# Migrar DB (garantia extra)
airflow db migrate

# Criar usuário admin (só se não existir ainda)
airflow users create \
    --username admin \
    --firstname Takeshy \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Iniciar scheduler em background
echo "Starting Airflow Scheduler..."
airflow scheduler > scheduler.log 2>&1 &

# Iniciar webserver em background
echo "Starting Airflow Webserver on port 8080..."
airflow webserver --port 8080 > webserver.log 2>&1 &

echo "✅ Airflow iniciado! Acesse: http://localhost:8080"
