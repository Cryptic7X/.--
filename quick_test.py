# quick_test.py
from src.exchanges.simple_exchange_fixed import SimpleExchangeManager

manager = SimpleExchangeManager()

for symbol in ['AB', 'KTA', 'ALEO', 'SOSO']:
    data, exchange = manager.fetch_ohlcv_with_fallback(symbol, '4h', 10)
    print(f"{symbol}: {'✅ Success' if data else '❌ Failed'} via {exchange}")
