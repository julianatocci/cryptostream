#!/bin/bash

echo "🚀 Iniciando pipeline CryptoStream..."

VENV_PATH="$HOME/cryptostream/venv/bin/python"
PROJECT_PATH="$HOME/cryptostream"

# WebSocket → Pub/Sub
nohup $VENV_PATH $PROJECT_PATH/main.py \
  >> $PROJECT_PATH/ws.log 2>&1 &

echo "✅ WebSocket → Pub/Sub iniciado"

# Pub/Sub → BigQuery
nohup $VENV_PATH $PROJECT_PATH/pubsub_to_bq.py \
  >> $PROJECT_PATH/bq.log 2>&1 &

echo "✅ Pub/Sub → BigQuery iniciado"

echo "📌 Pipeline rodando em background"
