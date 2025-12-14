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
    "trades-sub": ("raw_trades", "trade"),
    "candles-sub": ("raw_candles", "candles"),
    "orderbook-sub": ("raw_orderbook", "orderbook"),
    "tickers-sub": ("raw_tickers", "tickers")
}

subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client()


def parse_event_ts(payload, type_):
    """Extrai o timestamp do evento dependendo do tipo da mensagem."""
    try:
        if type_ == "trade":
            return datetime.fromtimestamp(payload["T"] / 1000, tz=timezone.utc).isoformat()
        elif type_ == "orderbook":
            return datetime.fromtimestamp(payload["E"] / 1000, tz=timezone.utc).isoformat()
        elif type_ == "candles":
            return datetime.fromtimestamp(payload["k"]["T"] / 1000, tz=timezone.utc).isoformat()
        elif type_ == "tickers":
            return datetime.fromtimestamp(payload["E"] / 1000, tz=timezone.utc).isoformat()
    except:
        pass
    return datetime.now(timezone.utc).isoformat()


def insert_raw_bq(message, table_name, type_):
    """Função genérica para inserir mensagens no BigQuery."""
    try:
        payload = json.loads(message.data.decode("utf-8"))

        if type_ == "trade":
            symbol = payload.get("s", "").lower()
            event_type = payload.get("e", "")
            stream_value = f"{symbol}@{event_type}"

            row = {
                "stream": stream_value,
                "event_ts": parse_event_ts(payload, type_),
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
                "payload": json.dumps(payload)
            }

        elif type_ == "orderbook":
            symbol = payload.get("s", "").lower()
            stream_value = f"{symbol}@{payload.get('e','')}"
            row = {
                "stream": stream_value,
                "event_ts": parse_event_ts(payload, type_),
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
                "payload": json.dumps(payload)
            }

        elif type_ in ["candles", "tickers"]:
            row = {
                "s": payload.get("s", "").lower(),
                "event_ts": parse_event_ts(payload, type_),
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
                "payload": json.dumps(payload)
            }

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        errors = bq_client.insert_rows_json(table_id, [row])

        if errors:
            print(f"[{table_name}] Erro ao inserir no BQ:", errors)
        else:
            message.ack()
            print(f"[{table_name}] Inserido com sucesso!")

    except Exception as e:
        print(f"[{table_name}] Erro ao processar mensagem:", e)


def create_callback(table_name, type_):
    """Cria função de callback para cada subscription."""
    def callback(message):
        insert_raw_bq(message, table_name, type_)
    return callback


for sub_name, (table_name, type_) in SUBSCRIPTIONS.items():
    subscription_path = subscriber.subscription_path(PROJECT_ID, sub_name)
    subscriber.subscribe(subscription_path, callback=create_callback(table_name, type_))
    print(f"[{sub_name}] Listening… tabela: {table_name}, tipo: {type_}")

print("\n[pubsub_to_bq] Monitoramento iniciado.\n")

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print("Encerrado pelo usuário.")
