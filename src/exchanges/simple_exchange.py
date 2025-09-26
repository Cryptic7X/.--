"""
Simple Exchange Layer - BingX + Public API Fallbacks
UPDATED VERSION - Added 15M and 4H timeframe support for EMA testing
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

    def fetch_bingx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from BingX (authenticated) - UPDATED with 15M + 4H"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        
        # UPDATED BingX interval mapping
        interval_map = {
            '15m': '15m',    # Added for EMA testing
            '30m': '30m',
            '2h': '2h',
            '4h': '4h'       # Added for EMA production
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
        """Fetch data from KuCoin (public API) - UPDATED with 15M + 4H"""
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        # UPDATED KuCoin interval mapping
        interval_map = {
            '15m': '15min',   # Added for EMA testing
            '30m': '30min',
            '2h': '2hour',
            '4h': '4hour'     # Added for EMA production
        }

        # Calculate time range based on timeframe
        end_time = int(time.time())
        timeframe_minutes = {
            '15m': 15,
            '30m': 30,
            '2h': 120,
            '4h': 240
        }
        
        minutes = timeframe_minutes.get(timeframe, 30)
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

        except Exception as e:
            print(f"⚠️ KuCoin failed for {symbol}: {str(e)[:50]}")
            return None

    def fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from OKX (public API) - UPDATED with 15M + 4H"""
        url = "https://www.okx.com/api/v5/market/candles"
        
        # UPDATED OKX interval mapping
        interval_map = {
            '15m': '15m',     # Added for EMA testing
            '30m': '30m',
            '2h': '2H',
            '4h': '4H'        # Added for EMA production
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
        """FINAL FIXED VERSION - Handles all exchange formats correctly"""
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
                    if exchange == 'bingx':
                        # FIXED: BingX format is DICTIONARY, not list!
                        if isinstance(candle, dict):
                            # BingX dictionary format
                            timestamp = int(float(candle.get('time', 0)))
                            open_price = float(candle.get('open', 0))
                            high_price = float(candle.get('high', 0))
                            low_price = float(candle.get('low', 0))
                            close_price = float(candle.get('close', 0))
                            volume = float(candle.get('volume', 0))
                        
                        elif isinstance(candle, (list, tuple)) and len(candle) >= 6:
                            # BingX list format (fallback)
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

                except (ValueError, TypeError, KeyError) as e:
                    continue

            # Ensure we have data
            if len(normalized_data['timestamp']) == 0:
                return None

            return normalized_data

        except Exception as e:
            print(f"⚠️ Data normalization failed for {exchange}: {str(e)}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Fetch OHLCV data with fallback chain: BingX → KuCoin → OKX
        NOW SUPPORTS: 15m, 30m, 2h, 4h
        Returns (data, exchange_used)
        """
        # Validate supported timeframes
        supported_timeframes = ['15m', '30m', '2h', '4h']
        if timeframe not in supported_timeframes:
            print(f"❌ Unsupported timeframe: {timeframe}. Supported: {supported_timeframes}")
            return None, None

        # Apply symbol mapping
        api_symbol, display_symbol = self.apply_symbol_mapping(symbol)

        # Try BingX first (authenticated)
        data = self.fetch_bingx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"✅ {symbol} {timeframe} data from BingX ({len(data['timestamp'])} candles)")
            return data, 'BingX'

        # Try KuCoin (public)
        data = self.fetch_kucoin_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"✅ {symbol} {timeframe} data from KuCoin ({len(data['timestamp'])} candles)")
            return data, 'KuCoin'

        # Try OKX (public)
        data = self.fetch_okx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"✅ {symbol} {timeframe} data from OKX ({len(data['timestamp'])} candles)")
            return data, 'OKX'

        print(f"❌ All exchanges failed for {symbol} ({api_symbol}) {timeframe}")
        return None, None
