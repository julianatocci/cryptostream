# como iniciar e para o pipeline de dados
# ./scripts/start_pipeline.sh
# ./scripts/stop_pipeline.sh

import os
import asyncio
import json
import websockets
from google.cloud import pubsub_v1
from datetime import datetime
from symbols_extract import get_binance_symbols

PROJECT_ID = "wagon-bootcamp-466414"

TOPICS = {
    "trade": f"projects/{PROJECT_ID}/topics/trades-topic",
    "kline_1m": f"projects/{PROJECT_ID}/topics/candles-topic",
    "depth": f"projects/{PROJECT_ID}/topics/orderbook-topic",
    "ticker": f"projects/{PROJECT_ID}/topics/tickers-topic",
}

publisher = pubsub_v1.PublisherClient()

BATCH_SIZE = 300

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

async def send_to_pubsub(topic, data):
    try:
        publisher.publish(topic, json.dumps(data).encode("utf-8"))
    except Exception as e:
        print(f"[{datetime.now()}] Erro Pub/Sub ({topic}): {e}")


async def consume_combined_stream(streams, topic_name, batch_id):
    stream_path = "/".join(streams)
    url = f"wss://stream.binance.com:9443/stream?streams={stream_path}"

    retry = 1

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print(
                    f"[{datetime.now()}] "
                    f"Conectado | {topic_name} | batch {batch_id} | "
                    f"{len(streams)} streams"
                )

                retry = 1

                async for msg in ws:
                    payload = json.loads(msg)
                    await send_to_pubsub(TOPICS[topic_name], payload)

        except Exception as e:
            print(
                f"[{datetime.now()}] "
                f"Erro WS | {topic_name} | batch {batch_id} | {e}"
            )

        await asyncio.sleep(retry)
        retry = min(retry * 2, 60)


async def main():
    symbols = get_binance_symbols()
    print(f"[INFO] {len(symbols)} símbolos ativos carregados")

    streams_by_type = {
        "trade":   [f"{s}@trade" for s in symbols],
        "kline_1m": [f"{s}@kline_1m" for s in symbols],
        "depth":   [f"{s}@depth" for s in symbols],
        "ticker":  [f"{s}@ticker" for s in symbols],
    }

    tasks = []

    for stream_type, streams in streams_by_type.items():
        batches = list(chunk_list(streams, BATCH_SIZE))

        print(
            f"[INFO] {stream_type}: "
            f"{len(streams)} streams → {len(batches)} batches"
        )

        for idx, batch in enumerate(batches):
            tasks.append(
                consume_combined_stream(
                    batch,
                    stream_type,
                    batch_id=idx
                )
            )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        print(f"[{datetime.now()}] Iniciando Binance → Pub/Sub (ALL SYMBOLS)")
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Encerrado pelo usuário")
