import requests

def get_binance_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    return [
        s["symbol"].lower()
        for s in data["symbols"]
        if s["status"] == "TRADING"
    ]

if __name__ == "__main__":
    symbols = get_binance_symbols()
    print(symbols)
