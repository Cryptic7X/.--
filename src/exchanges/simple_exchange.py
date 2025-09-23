"""
Simple Exchange Layer - BingX + Public API Fallbacks
COMPLETE DEBUG VERSION - Will show exactly what data format we're getting
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
        """Fetch data from BingX (authenticated) - WITH DEBUG"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            print(f"⚠️ BingX API key not found for {symbol}")
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
            print(f"🔍 DEBUG BingX: Requesting {symbol}-USDT {timeframe}")
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            print(f"🔍 DEBUG BingX Response for {symbol}:")
            print(f"   Status Code: {response.status_code}")
            print(f"   Response Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            if data.get('code') == 0:
                raw_data = data.get('data', [])
                print(f"   Data Length: {len(raw_data) if raw_data else 0}")
                if raw_data and len(raw_data) > 0:
                    print(f"   First Candle: {raw_data[0]}")
                    print(f"   First Candle Length: {len(raw_data[0]) if isinstance(raw_data[0], (list, tuple)) else 'Not a list'}")
                
                return self.normalize_ohlcv_data(raw_data, 'bingx')
            else:
                print(f"   BingX Error Code: {data.get('code')} - {data.get('msg', 'Unknown error')}")
                return None
            
        except Exception as e:
            print(f"❌ BingX API error for {symbol}: {str(e)}")
            return None

    def fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from KuCoin (public API)"""
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
            print(f"🔍 DEBUG KuCoin: Requesting {symbol}-USDT {timeframe}")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '200000' and data.get('data'):
                raw_data = data['data']
                print(f"   KuCoin Success: {len(raw_data)} candles")
                return self.normalize_ohlcv_data(raw_data, 'kucoin')
            else:
                print(f"   KuCoin Error: {data.get('code')} - {data.get('msg', 'Unknown error')}")
                return None
            
        except Exception as e:
            print(f"❌ KuCoin API error for {symbol}: {str(e)}")
            return None

    def fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from OKX (public API)"""
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
            print(f"🔍 DEBUG OKX: Requesting {symbol}-USDT {timeframe}")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '0' and data.get('data'):
                raw_data = data['data']
                print(f"   OKX Success: {len(raw_data)} candles")
                return self.normalize_ohlcv_data(raw_data, 'okx')
            else:
                print(f"   OKX Error: {data.get('code')} - {data.get('msg', 'Unknown error')}")
                return None
            
        except Exception as e:
            print(f"❌ OKX API error for {symbol}: {str(e)}")
            return None

    def normalize_ohlcv_data(self, raw_data: list, exchange: str) -> Optional[Dict]:
        """COMPLETE DEBUG VERSION - Shows exactly what's happening"""
        print(f"🔧 NORMALIZE DEBUG ({exchange}):")
        print(f"   Raw data type: {type(raw_data)}")
        print(f"   Raw data length: {len(raw_data) if raw_data else 'None/Empty'}")
        
        if not raw_data or len(raw_data) == 0:
            print(f"   ❌ No data to normalize")
            return None

        # Show first few items for debugging
        if len(raw_data) > 0:
            print(f"   First item: {raw_data[0]}")
            print(f"   First item type: {type(raw_data[0])}")
            if isinstance(raw_data[0], (list, tuple)):
                print(f"   First item length: {len(raw_data[0])}")

        normalized_data = {
            'timestamp': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': []
        }

        success_count = 0
        error_count = 0

        try:
            for i, candle in enumerate(raw_data):
                if not candle:
                    error_count += 1
                    continue
                    
                try:
                    if exchange == 'bingx':
                        # BingX format debugging
                        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                            print(f"   ⚠️ BingX candle {i}: Invalid format - {candle}")
                            error_count += 1
                            continue
                            
                        # BingX format: [timestamp, open, high, low, close, volume, ...]
                        timestamp = int(float(str(candle[0])))
                        open_price = float(str(candle[1]))
                        high_price = float(str(candle[2]))
                        low_price = float(str(candle[3]))
                        close_price = float(str(candle[4]))
                        volume = float(str(candle[5]))
                        
                        normalized_data['timestamp'].append(timestamp)
                        normalized_data['open'].append(open_price)
                        normalized_data['high'].append(high_price)
                        normalized_data['low'].append(low_price)
                        normalized_data['close'].append(close_price)
                        normalized_data['volume'].append(volume)
                        success_count += 1
                        
                    elif exchange == 'kucoin':
                        # KuCoin format: [timestamp, open, close, high, low, volume, turnover]
                        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                            error_count += 1
                            continue
                            
                        timestamp = int(float(str(candle[0])))
                        open_price = float(str(candle[1]))
                        high_price = float(str(candle[3]))
                        low_price = float(str(candle[4]))
                        close_price = float(str(candle[2]))
                        volume = float(str(candle[5]))
                        
                        normalized_data['timestamp'].append(timestamp)
                        normalized_data['open'].append(open_price)
                        normalized_data['high'].append(high_price)
                        normalized_data['low'].append(low_price)
                        normalized_data['close'].append(close_price)
                        normalized_data['volume'].append(volume)
                        success_count += 1
                        
                    elif exchange == 'okx':
                        # OKX format: [timestamp, open, high, low, close, volume, volumeCcy]
                        if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                            error_count += 1
                            continue
                            
                        timestamp = int(float(str(candle[0])))
                        open_price = float(str(candle[1]))
                        high_price = float(str(candle[2]))
                        low_price = float(str(candle[3]))
                        close_price = float(str(candle[4]))
                        volume = float(str(candle[5]))
                        
                        normalized_data['timestamp'].append(timestamp)
                        normalized_data['open'].append(open_price)
                        normalized_data['high'].append(high_price)
                        normalized_data['low'].append(low_price)
                        normalized_data['close'].append(close_price)
                        normalized_data['volume'].append(volume)
                        success_count += 1

                except (ValueError, TypeError, IndexError) as e:
                    print(f"   ⚠️ Error processing candle {i}: {e}")
                    error_count += 1
                    continue

            print(f"   ✅ Normalization complete: {success_count} success, {error_count} errors")
            
            # Ensure we have data
            if len(normalized_data['timestamp']) == 0:
                print(f"   ❌ No valid candles after normalization")
                return None

            print(f"   📊 Final data: {len(normalized_data['timestamp'])} candles")
            return normalized_data
            
        except Exception as e:
            print(f"   ❌ Normalization failed: {str(e)}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Fetch OHLCV data with fallback chain: BingX → KuCoin → OKX
        """
        print(f"\n🔄 Fetching {symbol} {timeframe} data...")
        
        # Apply symbol mapping
        api_symbol, display_symbol = self.apply_symbol_mapping(symbol)
        if api_symbol != display_symbol:
            print(f"   📍 Symbol mapped: {display_symbol} → {api_symbol}")
        
        # Try BingX first (authenticated)
        print(f"   1️⃣ Trying BingX...")
        data = self.fetch_bingx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"   ✅ BingX success: {len(data['timestamp'])} candles")
            return data, 'BingX'
        
        # Try KuCoin (public)
        print(f"   2️⃣ Trying KuCoin...")
        data = self.fetch_kucoin_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"   ✅ KuCoin success: {len(data['timestamp'])} candles")
            return data, 'KuCoin'
        
        # Try OKX (public)
        print(f"   3️⃣ Trying OKX...")
        data = self.fetch_okx_data(api_symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            print(f"   ✅ OKX success: {len(data['timestamp'])} candles")
            return data, 'OKX'
        
        print(f"   ❌ All exchanges failed for {symbol} ({api_symbol})")
        return None, None
