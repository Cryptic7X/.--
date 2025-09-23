"""
Simple Exchange Layer - BingX + Public API Fallbacks
FIXED: Data normalization and DataFrame conversion issues
"""

import os
import json
import time
import requests
import yaml
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

class SimpleExchangeManager:
    def __init__(self):
        self.config = self.load_config()
        self.symbol_mapping = self.load_symbol_mapping()
        self.session = self.create_session()

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)

    def load_symbol_mapping(self):
        mapping_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'symbol_mapping.json')
        try:
            with open(mapping_path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Multi-Indicator-Exchange/3.0',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        })
        return session

    def apply_symbol_mapping(self, symbol: str) -> Tuple[str, str]:
        """Apply symbol mapping and return (api_symbol, display_symbol)"""
        display_symbol = symbol.upper()
        api_symbol = self.symbol_mapping.get(display_symbol, display_symbol)
        return api_symbol, display_symbol

    def fetch_bingx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from BingX (authenticated) - FIXED"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        
        # BingX interval mapping
        interval_map = {
            '30m': '30m',
            '2h': '2h'
        }
        
        headers = {
            'X-BX-APIKEY': api_key,
            'Content-Type': 'application/json'
        }
        
        params = {
            'symbol': f'{symbol}-USDT',
            'interval': interval_map.get(timeframe, timeframe),
            'limit': limit
        }

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0 and data.get('data'):
                return self.normalize_ohlcv_data(data['data'], 'bingx')
            return None
            
        except Exception as e:
            print(f"⚠️ BingX failed for {symbol}: {str(e)[:50]}")
            return None

    def fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from KuCoin (public API) - FIXED"""
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        # KuCoin interval mapping
        interval_map = {
            '30m': '30min',
            '2h': '2hour'
        }
        
        # Calculate time range
        end_time = int(time.time())
        if timeframe == '30m':
            start_time = end_time - (limit * 30 * 60)
        else:  # 2h
            start_time = end_time - (limit * 2 * 60 * 60)
        
        params = {
            'symbol': f'{symbol}-USDT',
            'type': interval_map.get(timeframe, timeframe),
            'startAt': start_time,
            'endAt': end_time
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '200000' and data.get('data'):
                return self.normalize_ohlcv_data(data['data'], 'kucoin')
            return None
            
        except Exception as e:
            print(f"⚠️ KuCoin failed for {symbol}: {str(e)[:50]}")
            return None

    def fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from OKX (public API) - FIXED"""
        url = "https://www.okx.com/api/v5/market/candles"
        
        # OKX interval mapping
        interval_map = {
            '30m': '30m',
            '2h': '2H'
        }
        
        params = {
            'instId': f'{symbol}-USDT',
            'bar': interval_map.get(timeframe, timeframe),
            'limit': str(limit)
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '0' and data.get('data'):
                return self.normalize_ohlcv_data(data['data'], 'okx')
            return None
            
        except Exception as e:
            print(f"⚠️ OKX failed for {symbol}: {str(e)[:50]}")
            return None

    def normalize_ohlcv_data(self, raw_data: list, exchange: str) -> Optional[Dict]:
        """FIXED: Normalize OHLCV data from different exchanges"""
        if not raw_data or len(raw_data) == 0:
            return None

        normalized_data = {
            'timestamp': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': []
        }

        try:
            for candle in raw_data:
                if not candle or len(candle) < 6:
                    continue
                    
                if exchange == 'bingx':
                    # BingX format: [timestamp, open, high, low, close, volume, ...]
                    normalized_data['timestamp'].append(int(float(candle[0])))
                    normalized_data['open'].append(float(candle[1]))
                    normalized_data['high'].append(float(candle[2]))
                    normalized_data['low'].append(float(candle[3]))
                    normalized_data['close'].append(float(candle[4]))
                    normalized_data['volume'].append(float(candle[5]))
                    
                elif exchange == 'kucoin':
                    # KuCoin format: [timestamp, open, close, high, low, volume, turnover]
                    normalized_data['timestamp'].append(int(float(candle[0])))
                    normalized_data['open'].append(float(candle[1]))
                    normalized_data['high'].append(float(candle[3]))
                    normalized_data['low'].append(float(candle[4]))
                    normalized_data['close'].append(float(candle[2]))
                    normalized_data['volume'].append(float(candle[5]))
                    
                elif exchange == 'okx':
                    # OKX format: [timestamp, open, high, low, close, volume, volumeCcy]
                    normalized_data['timestamp'].append(int(float(candle[0])))
                    normalized_data['open'].append(float(candle[1]))
                    normalized_data['high'].append(float(candle[2]))
                    normalized_data['low'].append(float(candle[3]))
                    normalized_data['close'].append(float(candle[4]))
                    normalized_data['volume'].append(float(candle[5]))

            # Ensure we have data
            if len(normalized_data['timestamp']) == 0:
                return None

            return normalized_data
            
        except Exception as e:
            print(f"⚠️ Data normalization failed for {exchange}: {str(e)}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """
        FIXED: Fetch OHLCV data with fallback chain: BingX → KuCoin → OKX
        Returns (data, exchange_used)
        """
        # Apply symbol mapping
        api_symbol, display_symbol = self.apply_symbol_mapping(symbol)
        
        # Try BingX first (authenticated)
        data = self.fetch_bingx_data(api_symbol, timeframe, limit)
        if data and len(data['timestamp']) > 0:
            return data, 'BingX'
        
        # Try KuCoin (public)
        data = self.fetch_kucoin_data(api_symbol, timeframe, limit)
        if data and len(data['timestamp']) > 0:
            return data, 'KuCoin'
        
        # Try OKX (public)
        data = self.fetch_okx_data(api_symbol, timeframe, limit)
        if data and len(data['timestamp']) > 0:
            return data, 'OKX'
        
        print(f"❌ All exchanges failed for {symbol} ({api_symbol})")
        return None, None
