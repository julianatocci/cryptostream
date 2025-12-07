import os
import json
import time
from threading import Thread
from websocket import WebSocketApp
from google.cloud import pubsub_v1

BINANCE_STREAMS = {
    "btcusdt@trade": os.environ.get("PUBSUB_TOPIC_TRADES"),
    "btcusdt@kline_1m": os.environ.get("PUBSUB_TOPIC_CANDLES"),
    "btcusdt@depth": os.environ.get("PUBSUB_TOPIC_ORDERBOOK"),
    "btcusdt@ticker": os.environ.get("PUBSUB_TOPIC_TICKERS")
}

PROJECT_ID = os.environ.get("wagon-bootcamp-466414")

publisher = pubsub_v1.PublisherClient()

def create_ws_callbacks(topic_id):
    def on_open(ws):
        print(f"Conectado ao WebSocket ({topic_id})")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            future = publisher.publish(
                publisher.topic_path(PROJECT_ID, topic_id),
                json.dumps(data).encode("utf-8")
            )
            print(f"Publicado no Pub/Sub ({topic_id}): {future.result()}")
        except Exception as e:
            print(f"Erro ao publicar ({topic_id}): {e}")

    def on_error(ws, error):
        print(f"Erro no WebSocket ({topic_id}): {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"WebSocket fechado ({topic_id})")

    return on_open, on_message, on_error, on_close

def run_ws(stream, topic_id):
    url = f"wss://stream.binance.com:9443/ws/{stream}"
    on_open, on_message, on_error, on_close = create_ws_callbacks(topic_id)

    while True:
        ws = WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        print(f"Tentando reconectar {stream} em 5 segundos...")
        time.sleep(5)

if __name__ == "__main__":
    threads = []
    for stream, topic_id in BINANCE_STREAMS.items():
        if topic_id:
            t = Thread(target=run_ws, args=(stream, topic_id))
            t.start()
            threads.append(t)

    for t in threads:
        t.join()
