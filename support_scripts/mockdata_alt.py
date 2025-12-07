import random
import json
from datetime import datetime, timedelta, timezone

# =========================
# CONFIGURAÇÕES DO MOCK
# =========================
random.seed(42)

N_ROWS = 6000                        # total de linhas no bronze
START  = datetime(2025, 12, 3, 12, 0, 0, tzinfo=timezone.utc)

SYMBOLS = [
    {"symbol": "BTCUSDT", "base_price": 42000.0, "vol": 35.0},
    {"symbol": "ETHUSDT", "base_price": 2200.0,  "vol": 3.5},
    {"symbol": "BNBUSDT", "base_price": 310.0,   "vol": 0.9},
]

QTY_RANGES = {
    "BTCUSDT": (0.0001, 0.0030),
    "ETHUSDT": (0.0010, 0.0500),
    "BNBUSDT": (0.0100, 0.5000),
}

OUTFILE = "data/ws_raw_mock.jsonl"


def iso_z(dt: datetime) -> str:
    """Converte datetime aware para ISO com Z."""
    return dt.isoformat().replace("+00:00", "Z")


def main():
    cur_ts = START

    # preço corrente por símbolo (random walk independente)
    prices = {s["symbol"]: s["base_price"] for s in SYMBOLS}

    with open(OUTFILE, "w", encoding="utf-8") as f:
        for i in range(N_ROWS):
            # round-robin entre símbolos pra distribuição uniforme
            s = SYMBOLS[i % len(SYMBOLS)]
            symbol = s["symbol"]
            stream = f"{symbol.lower()}@trade"

            # avança tempo 1–3s por evento
            cur_ts += timedelta(seconds=random.randint(1, 3))

            # random walk no preço
            prices[symbol] += random.uniform(-s["vol"], s["vol"])
            price = prices[symbol]

            # tempo do evento (ms) com jitter leve
            event_ms = int(cur_ts.timestamp() * 1000) - random.randint(0, 400)
            E_ms = event_ms + random.randint(0, 20)

            qmin, qmax = QTY_RANGES[symbol]
            qty = random.uniform(qmin, qmax)

            payload = {
                "e": "trade",
                "E": E_ms,                # event time (ms)
                "s": symbol,              # symbol
                "t": 100000 + i,          # trade id
                "p": f"{price:.2f}",      # price string
                "q": f"{qty:.4f}",        # qty string
                "T": event_ms,            # trade time (ms)
                "m": random.choice([True, False]),
                "M": True                 # ignore (como docs)
            }

            row = {
                "stream": stream,
                "ingest_ts": iso_z(cur_ts),
                "payload": payload,  # JSON nativo (BigQuery JSON)
                "event_ts": iso_z(datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc))
            }

            f.write(json.dumps(row) + "\n")

    print(f"Gerado: {OUTFILE} com {N_ROWS} linhas.")
    print("Símbolos:", ", ".join(prices.keys()))


if __name__ == "__main__":
    main()
