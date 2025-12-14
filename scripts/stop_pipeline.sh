#!/bin/bash

echo "Parando pipeline CryptoStream..."

pkill -f main.py
pkill -f pubsub_to_bq.py

sleep 1

echo "Processos ainda ativos:"
ps aux | grep -E "main.py|pubsub_to_bq.py" | grep -v grep

echo "Pipeline encerrado"
