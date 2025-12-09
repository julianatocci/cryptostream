import random, json
from datetime import datetime, timedelta, timezone

random.seed(42)

PROJECT = "wagon-bootcamp-466414"
DATASET = "cryptostream"
TABLE   = "ws_raw_trades"

# --- CONFIG DO MOCK ---
N_ROWS = 6000  # total de linhas no bronze
START  = datetime(2025, 12, 8, 12, 0, 0, tzinfo=timezone.utc)

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

def make_row(i, cur_ts, symbol, price):
    stream = f"{symbol.lower()}@trade"

    # tempo do evento (ms) com jitter leve
    event_ms = int(cur_ts.timestamp() * 1000) - random.randint(0, 400)
    E_ms = event_ms + random.randint(0, 20)

    qmin, qmax = QTY_RANGES[symbol]
    qty = random.uniform(qmin, qmax)
    m = random.choice([True, False])

    payload = {
        "e": "trade",
        "E": E_ms,           # event time do WS
        "s": symbol,         # symbol
        "t": 100000 + i,     # trade id incremental
        "p": f"{price:.2f}", # price string
        "q": f"{qty:.4f}",   # qty string
        "T": event_ms,       # trade time (ms)
        "m": m,               # buyer is market maker?
        "M": True
    }

    return f'''(
  "{stream}",
  TIMESTAMP("{cur_ts.isoformat().replace("+00:00","Z")}"),
  JSON '{json.dumps(payload, separators=(",",":"))}',
  TIMESTAMP_MILLIS({event_ms})
)'''

def main():
    cur_ts = START

    # preço atual por símbolo
    prices = {s["symbol"]: s["base_price"] for s in SYMBOLS}

    tuples = []
    for i in range(N_ROWS):
        # escolhe símbolo em round-robin (fica bem distribuído)
        s = SYMBOLS[i % len(SYMBOLS)]
        symbol = s["symbol"]

        # avança tempo 1–3s por evento
        cur_ts += timedelta(seconds=random.randint(1, 3))

        # random walk no preço do símbolo
        prices[symbol] += random.uniform(-s["vol"], s["vol"])

        tuples.append(make_row(i, cur_ts, symbol, prices[symbol]))

    sql = f"""INSERT INTO `{PROJECT}.{DATASET}.{TABLE}`
(stream, ingest_ts, payload, event_ts)
VALUES
{",\n".join(tuples)};
"""

    with open("mock_trades_multisymbol_insert.sql", "w", encoding="utf-8") as f:
        f.write(sql)

    print("Arquivo gerado: mock_trades_multisymbol_insert.sql")
    print(f"Linhas: {N_ROWS}")
    print("Símbolos:", ", ".join(prices.keys()))

if __name__ == "__main__":
    main()
