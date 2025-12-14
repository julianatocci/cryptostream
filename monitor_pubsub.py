import os
import threading
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/juliana.tocci/cryptostream/wagon-bootcamp-466414-0ee6a728ccaa.json"

PROJECT_ID = "wagon-bootcamp-466414"

SUBSCRIPTIONS = [
    "trades-sub",
    "candles-sub",
    "orderbook-sub",
    "tickers-sub"
]

def monitor_subscription(subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_id)

    def callback(message):
        print(f"[{subscription_id}] Nova mensagem:")
        print(message.data.decode("utf-8"))
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"[{subscription_id}] Escutando mensagens...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

threads = []
for sub in SUBSCRIPTIONS:
    t = threading.Thread(target=monitor_subscription, args=(sub,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
