import os
import json
import time
from google.cloud import pubsub_v1, bigquery
from datetime import datetime, timezone

# Caminho do JSON de credenciais
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/juliana.tocci/cryptostream/wagon-bootcamp-466414-0ee6a728ccaa.json"

PROJECT_ID = "wagon-bootcamp-466414"
DATASET_ID = "crypto_data_raw"

SUBSCRIPTIONS = {
    "trades-sub": "trades_raw",
    "candles-sub": "candles_raw",
    "orderbook-sub": "orderbook_raw",
    "tickers-sub": "tickers_raw"
}

subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client()

def create_table_if_not_exists(table_name):
    dataset_ref = bq_client.dataset(DATASET_ID)

    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        print(f"[BQ] Dataset '{DATASET_ID}' não existe. Criando...")
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"[BQ] Dataset '{DATASET_ID}' criado.")

    table_ref = dataset_ref.table(table_name)

    try:
        bq_client.get_table(table_ref)
        print(f"[BQ] Tabela '{table_name}' já existe.")
    except Exception:
        print(f"[BQ] Tabela '{table_name}' não existe. Criando...")
        schema = [
            bigquery.SchemaField("received_at", "TIMESTAMP"),
            bigquery.SchemaField("data", "STRING")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"[BQ] Tabela '{table_name}' criada.")

def callback_factory(table_name):
    def callback(message):
        try:
            data = json.loads(message.data.decode("utf-8"))

            data_row = {
                "received_at": datetime.now(timezone.utc).isoformat(),
                "data": json.dumps(data)
            }

            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            errors = bq_client.insert_rows_json(table_id, [data_row])
            if errors:
                print(f"[{table_name}] Erro ao inserir no BQ: {errors}")
            else:
                message.ack()
                print(f"[{table_name}] Mensagem inserida no BQ")
        except Exception as e:
            print(f"[{table_name}] Erro ao processar mensagem: {e}")
    return callback

for sub_name, table_name in SUBSCRIPTIONS.items():
    create_table_if_not_exists(table_name)
    subscription_path = subscriber.subscription_path(PROJECT_ID, sub_name)
    subscriber.subscribe(subscription_path, callback=callback_factory(table_name))
    print(f"[{sub_name}] Listening...")

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print(f"[{datetime.now()}] Script encerrado pelo usuário")
