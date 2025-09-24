"""
Simple Exchange Manager - OPTIMIZED FOR SPEED
Faster timeouts and error handling
"""
import os
import json
import time
import requests
import yaml

class SimpleExchangeManager:
    def __init__(self):
        self.symbol_mapping = {}  # Skip mapping for speed
        self.session = requests.Session()
        
        # Optimized session settings
        self.session.timeout = 5  # Faster timeout
        adapter = requests.adapters.HTTPAdapter(max_retries=0)  # No retries
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _fetch_bingx_data(self, symbol: str, limit: int = 100):
        """Fast BingX fetch - reduced limit"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        try:
            response = self.session.get(
                "https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                headers={'X-BX-APIKEY': api_key},
                params={
                    'symbol': f'{symbol}-USDT',
                    'interval': '2h',
                    'limit': limit
                },
                timeout=5
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            if data.get('code') != 0:
                return None
                
            raw_data = data.get('data', [])
            if not raw_data:
                return None

            # Fast normalization
            return {
                'timestamp': [int(float(candle.get('time', 0))) for candle in raw_data],
                'open': [float(candle.get('open', 0)) for candle in raw_data],
                'high': [float(candle.get('high', 0)) for candle in raw_data],
                'low': [float(candle.get('low', 0)) for candle in raw_data],
                'close': [float(candle.get('close', 0)) for candle in raw_data],
                'volume': [float(candle.get('volume', 0)) for candle in raw_data]
            }
            
        except:
            return None

    def _fetch_kucoin_data(self, symbol: str, limit: int = 100):
        """Fast KuCoin fetch"""
        try:
            end_time = int(time.time())
            start_time = end_time - (limit * 2 * 60 * 60)

            response = self.session.get(
                "https://api.kucoin.com/api/v1/market/candles",
                params={
                    'symbol': f'{symbol}-USDT',
                    'type': '2hour',
                    'startAt': start_time,
                    'endAt': end_time
                },
                timeout=5
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            if data.get('code') != '200000':
                return None
                
            raw_data = data.get('data', [])
            if not raw_data:
                return None

            # Fast normalization
            return {
                'timestamp': [int(float(candle[0])) for candle in raw_data],
                'open': [float(candle[1]) for candle in raw_data],
                'high': [float(candle[3]) for candle in raw_data],
                'low': [float(candle[4]) for candle in raw_data],
                'close': [float(candle[2]) for candle in raw_data],
                'volume': [float(candle[5]) for candle in raw_data]
            }
            
        except:
            return None

    def _fetch_okx_data(self, symbol: str, limit: int = 100):
        """Fast OKX fetch"""
        try:
            response = self.session.get(
                "https://www.okx.com/api/v5/market/candles",
                params={
                    'instId': f'{symbol}-USDT',
                    'bar': '2H',
                    'limit': str(limit)
                },
                timeout=5
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            if data.get('code') != '0':
                return None
                
            raw_data = data.get('data', [])
            if not raw_data:
                return None

            # Fast normalization
            return {
                'timestamp': [int(float(candle[0])) for candle in raw_data],
                'open': [float(candle[1]) for candle in raw_data],
                'high': [float(candle[2]) for candle in raw_data],
                'low': [float(candle[3]) for candle in raw_data],
                'close': [float(candle[4]) for candle in raw_data],
                'volume': [float(candle[5]) for candle in raw_data]
            }
            
        except:
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 100):
        """Fast OHLCV fetch with minimal fallback"""
        if timeframe != '2h':
            return None, None

        # Try BingX first (usually fastest)
        data = self._fetch_bingx_data(symbol, limit)
        if data:
            return data, 'BingX'
        
        # Try KuCoin
        data = self._fetch_kucoin_data(symbol, limit)
        if data:
            return data, 'KuCoin'
        
        # Try OKX
        data = self._fetch_okx_data(symbol, limit)
        if data:
            return data, 'OKX'
        
        return None, None
