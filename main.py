import os
import asyncio
import json
import websockets
from google.cloud import pubsub_v1
from datetime import datetime

# Configurações do projeto e tópicos Pub/Sub
PROJECT_ID = "wagon-bootcamp-466414"
TOPICS = {
    "btcusdt@trade": f"projects/{PROJECT_ID}/topics/trades-topic",
    "btcusdt@kline_1m": f"projects/{PROJECT_ID}/topics/candles-topic",
    "btcusdt@depth": f"projects/{PROJECT_ID}/topics/orderbook-topic",
    "btcusdt@ticker": f"projects/{PROJECT_ID}/topics/tickers-topic"
}

publisher = pubsub_v1.PublisherClient()

async def send_to_pubsub(topic, data):
    """Envia dados para o Pub/Sub"""
    try:
        publisher.publish(topic, json.dumps(data).encode("utf-8"))
    except Exception as e:
        print(f"[{datetime.now()}] Erro ao publicar no Pub/Sub ({topic}): {e}")

async def binance_ws(stream, topic):
    """Conecta no WebSocket da Binance e envia dados para Pub/Sub"""
    url = f"wss://stream.binance.com:9443/ws/{stream}"
    retry_seconds = 1
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"[{datetime.now()}] Conectado no stream {stream}")
                retry_seconds = 1  # reset retry time
                async for msg in ws:
                    data = json.loads(msg)
                    await send_to_pubsub(topic, data)
        except websockets.ConnectionClosedError as e:
            print(f"[{datetime.now()}] Stream {stream} fechado: {e}. Reconectando em {retry_seconds}s...")
        except Exception as e:
            print(f"[{datetime.now()}] Erro no stream {stream}: {e}. Reconectando em {retry_seconds}s...")

        await asyncio.sleep(retry_seconds)
        # aumenta exponencialmente o tempo de reconexão até 1 minuto
        retry_seconds = min(retry_seconds * 2, 60)

async def main_loop():
    """Cria tasks para cada stream em paralelo"""
    tasks = []
    for stream, topic in TOPICS.items():
        tasks.append(binance_ws(stream, topic))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        print(f"[{datetime.now()}] Iniciando script Binance -> Pub/Sub")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Script encerrado pelo usuário")
