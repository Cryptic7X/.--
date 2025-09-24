"""Simple Exchange Manager - 30M + 2H Only (15M REMOVED)"""
import os
import json
import time
import requests
import yaml

class SimpleExchangeManager:
    def __init__(self):
        self.config = self._load_config()
        self.symbol_mapping = self._load_symbol_mapping()
        self.session = requests.Session()

    def _load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _load_symbol_mapping(self):
        mapping_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'symbol_mapping.json')
        try:
            with open(mapping_path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _fetch_bingx_data(self, symbol: str, timeframe: str, limit: int = 200):
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        
        # UPDATED: 15m removed, 30m added
        interval_map = {'30m': '30m', '2h': '2h'}
        if timeframe not in interval_map:
            return None

        headers = {'X-BX-APIKEY': api_key}
        params = {
            'symbol': f'{symbol}-USDT',
            'interval': interval_map[timeframe],
            'limit': limit
        }

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0 and data.get('data'):
                return self._normalize_data(data['data'], 'bingx')
        except:
            pass
        return None

    def _fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200):
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        # UPDATED: 15m removed, 30m added
        interval_map = {'30m': '30min', '2h': '2hour'}
        if timeframe not in interval_map:
            return None

        end_time = int(time.time())
        if timeframe == '30m':
            start_time = end_time - (limit * 30 * 60)  # 30 minutes
        else:  # 2h
            start_time = end_time - (limit * 2 * 60 * 60)

        params = {
            'symbol': f'{symbol}-USDT',
            'type': interval_map[timeframe],
            'startAt': start_time,
            'endAt': end_time
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '200000' and data.get('data'):
                return self._normalize_data(data['data'], 'kucoin')
        except:
            pass
        return None

    def _fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200):
        url = "https://www.okx.com/api/v5/market/candles"
        
        # UPDATED: 15m removed, 30m added  
        interval_map = {'30m': '30m', '2h': '2H'}
        if timeframe not in interval_map:
            return None

        params = {
            'instId': f'{symbol}-USDT',
            'bar': interval_map[timeframe],
            'limit': str(limit)
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '0' and data.get('data'):
                return self._normalize_data(data['data'], 'okx')
        except:
            pass
        return None

    def _normalize_data(self, raw_data, exchange):
        if not raw_data:
            return None

        normalized = {
            'timestamp': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': []
        }

        try:
            for candle in raw_data:
                if not candle:
                    continue
                    
                if exchange == 'bingx':
                    if isinstance(candle, dict):
                        normalized['timestamp'].append(int(float(candle.get('time', 0))))
                        normalized['open'].append(float(candle.get('open', 0)))
                        normalized['high'].append(float(candle.get('high', 0)))
                        normalized['low'].append(float(candle.get('low', 0)))
                        normalized['close'].append(float(candle.get('close', 0)))
                        normalized['volume'].append(float(candle.get('volume', 0)))
                        
                elif exchange == 'kucoin':
                    if len(candle) >= 6:
                        normalized['timestamp'].append(int(float(candle[0])))
                        normalized['open'].append(float(candle[1]))
                        normalized['high'].append(float(candle[3]))
                        normalized['low'].append(float(candle[4]))
                        normalized['close'].append(float(candle[2]))
                        normalized['volume'].append(float(candle[5]))
                        
                elif exchange == 'okx':
                    if len(candle) >= 6:
                        normalized['timestamp'].append(int(float(candle[0])))
                        normalized['open'].append(float(candle[1]))
                        normalized['high'].append(float(candle[2]))
                        normalized['low'].append(float(candle[3]))
                        normalized['close'].append(float(candle[4]))
                        normalized['volume'].append(float(candle[5]))

            return normalized if normalized['timestamp'] else None
            
        except Exception as e:
            print(f"Data normalization failed for {exchange}: {e}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200):
        # UPDATED: 15m removed, 30m added
        if timeframe not in ['30m', '2h']:
            return None, None

        api_symbol = self.symbol_mapping.get(symbol.upper(), symbol.upper())
        
        # Try BingX first
        data = self._fetch_bingx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'BingX'
        
        # Try KuCoin
        data = self._fetch_kucoin_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'KuCoin'
        
        # Try OKX
        data = self._fetch_okx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'OKX'
        
        return None, None
