"""
Simple Exchange Layer - BingX + Public API Fallbacks
FIXED VERSION - Correct BingX Spot symbol format (BTC-USDT)
"""
import os
import json
import time
import requests
import yaml
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

    def fetch_bingx_perpetuals_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch from BingX Perpetuals (Swap API)"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        
        # Updated interval mapping for all timeframes
        interval_map = {
            '15m': '15m',
            '2h': '2h',
            '4h': '4h',
            '8h': '8h'
        }

        headers = {
            'X-BX-APIKEY': api_key,
            'Content-Type': 'application/json'
        }

        params = {
            'symbol': f'{symbol}-USDT',  # Perpetuals format: BTC-USDT
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
            
        except Exception:
            # Silent fail - will try spot
            return None

    def fetch_bingx_spot_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch from BingX Spot API - FIXED SYMBOL FORMAT"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/spot/v1/market/kline"
        
        # BingX Spot interval mapping
        interval_map = {
            '15m': '15m',
            '2h': '2h',
            '4h': '4h',
            '8h': '8h'
        }

        headers = {
            'X-BX-APIKEY': api_key,
            'Content-Type': 'application/json'
        }

        params = {
            'symbol': f'{symbol}-USDT',  # FIXED: Spot also uses BTC-USDT format, not BTCUSDT
            'interval': interval_map.get(timeframe, timeframe),
            'limit': limit
        }

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0 and data.get('data'):
                return self.normalize_ohlcv_data(data['data'], 'bingx_spot')
            return None
            
        except Exception:
            return None

    def fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from KuCoin (public API) - Updated timeframes"""
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        # Updated KuCoin interval mapping
        interval_map = {
            '15m': '15min',
            '2h': '2hour',
            '4h': '4hour',
            '8h': '8hour'
        }

        # Calculate time range - Updated
        end_time = int(time.time())
        timeframe_minutes = {
            '15m': 15,
            '2h': 120,
            '4h': 240,
            '8h': 480
        }
        
        minutes = timeframe_minutes.get(timeframe, 120)
        start_time = end_time - (limit * minutes * 60)

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
            
        except Exception:
            return None

    def fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from OKX (public API) - Updated timeframes"""
        url = "https://www.okx.com/api/v5/market/candles"
        
        # Updated OKX interval mapping
        interval_map = {
            '15m': '15m',
            '2h': '2H',
            '4h': '4H',
            '8h': '8H'
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
            
        except Exception:
            return None

    def normalize_ohlcv_data(self, raw_data: list, exchange: str) -> Optional[Dict]:
        """Normalize OHLCV data - Updated for BingX Spot"""
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
                if not candle:
                    continue

                try:
                    if exchange in ['bingx', 'bingx_spot']:
                        # BingX format (both perpetuals and spot use same format)
                        if isinstance(candle, dict):
                            # Dictionary format
                            timestamp = int(float(candle.get('time', 0)))
                            open_price = float(candle.get('open', 0))
                            high_price = float(candle.get('high', 0))
                            low_price = float(candle.get('low', 0))
                            close_price = float(candle.get('close', 0))
                            volume = float(candle.get('volume', 0))
                            
                        elif isinstance(candle, (list, tuple)) and len(candle) >= 6:
                            # List format (fallback)
                            timestamp = int(float(candle[0]))
                            open_price = float(candle[1])
                            high_price = float(candle[2])
                            low_price = float(candle[3])
                            close_price = float(candle[4])
                            volume = float(candle[5])
                        else:
                            continue

                    elif exchange == 'kucoin':
                        # KuCoin format: [timestamp, open, close, high, low, volume, turnover]
                        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                            continue
                        timestamp = int(float(candle[0]))
                        open_price = float(candle[1])
                        high_price = float(candle[3])
                        low_price = float(candle[4])
                        close_price = float(candle[2])
                        volume = float(candle[5])

                    elif exchange == 'okx':
                        # OKX format: [timestamp, open, high, low, close, volume, volumeCcy]
                        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                            continue
                        timestamp = int(float(candle[0]))
                        open_price = float(candle[1])
                        high_price = float(candle[2])
                        low_price = float(candle[3])
                        close_price = float(candle[4])
                        volume = float(candle[5])

                    else:
                        continue

                    # Add normalized data
                    normalized_data['timestamp'].append(timestamp)
                    normalized_data['open'].append(open_price)
                    normalized_data['high'].append(high_price)
                    normalized_data['low'].append(low_price)
                    normalized_data['close'].append(close_price)
                    normalized_data['volume'].append(volume)

                except (ValueError, TypeError, KeyError):
                    continue

            # Ensure we have data
            if len(normalized_data['timestamp']) == 0:
                return None

            return normalized_data

        except Exception:
            return None

    def get_supported_timeframes(self) -> list:
        """Return list of supported timeframes"""
        return ['15m', '2h', '4h', '8h']

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Enhanced fallback chain: BingX Perpetuals → BingX Spot → KuCoin → OKX
        Returns (data, exchange_used)
        """
        # Validate timeframe
        if timeframe not in self.get_supported_timeframes():
            return None, None

        # Apply symbol mapping
        api_symbol, display_symbol = self.apply_symbol_mapping(symbol)

        # 1. Try BingX Perpetuals first
        data = self.fetch_bingx_perpetuals_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'BingX Perpetuals'

        # 2. Try BingX Spot (FIXED: Now uses correct BTC-USDT format)
        data = self.fetch_bingx_spot_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'BingX Spot'

        # 3. Try KuCoin (public)
        data = self.fetch_kucoin_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'KuCoin'

        # 4. Try OKX (public)
        data = self.fetch_okx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'OKX'

        return None, None
