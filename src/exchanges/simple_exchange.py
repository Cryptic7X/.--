"""
Simple Exchange Manager - QUICK FIX VERSION
Debug version with verbose output for problematic coins
"""
import os
import json
import time
import requests
import yaml
from typing import Optional, Tuple, Dict, Any

class SimpleExchangeManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Multi-Indicator-Exchange/3.0',
            'Accept': 'application/json'
        })

    def fetch_bingx_spot_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch from BingX Spot API with DEBUG"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            print(f"❌ No BingX API key for {symbol}")
            return None

        url = "https://open-api.bingx.com/openApi/spot/v1/market/kline"
        
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
            'symbol': f'{symbol}USDT',  # No hyphen for spot
            'interval': interval_map.get(timeframe, timeframe),
            'limit': limit
        }

        try:
            print(f"🔄 BingX Spot: {symbol} -> {params['symbol']}")
            
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ BingX Spot {symbol}: HTTP {response.status_code}")
                return None
                
            data = response.json()
            
            if data.get('code') == 0 and data.get('data'):
                print(f"✅ BingX Spot {symbol}: {len(data['data'])} candles")
                return self.normalize_ohlcv_data(data['data'], 'bingx_spot')
            else:
                print(f"❌ BingX Spot {symbol}: {data.get('msg', 'No data')}")
                return None
            
        except Exception as e:
            print(f"❌ BingX Spot {symbol} error: {e}")
            return None

    def fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch from KuCoin with DEBUG"""
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        interval_map = {
            '15m': '15min',
            '2h': '2hour',
            '4h': '4hour',
            '8h': '8hour'
        }

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
            print(f"🔄 KuCoin: {symbol} -> {params['symbol']}")
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ KuCoin {symbol}: HTTP {response.status_code}")
                return None
                
            data = response.json()
            
            if data.get('code') == '200000' and data.get('data'):
                print(f"✅ KuCoin {symbol}: {len(data['data'])} candles")
                return self.normalize_ohlcv_data(data['data'], 'kucoin')
            else:
                print(f"❌ KuCoin {symbol}: {data.get('msg', 'No data')}")
                return None
                
        except Exception as e:
            print(f"❌ KuCoin {symbol} error: {e}")
            return None

    def normalize_ohlcv_data(self, raw_data: list, exchange: str) -> Optional[Dict]:
        """Normalize OHLCV data"""
        if not raw_data:
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
                    if exchange == 'bingx_spot':
                        # BingX Spot format
                        if isinstance(candle, dict):
                            timestamp = int(float(candle.get('time', 0)))
                            open_price = float(candle.get('open', 0))
                            high_price = float(candle.get('high', 0))
                            low_price = float(candle.get('low', 0))
                            close_price = float(candle.get('close', 0))
                            volume = float(candle.get('volume', 0))
                        else:
                            continue

                    elif exchange == 'kucoin':
                        # KuCoin format: [timestamp, open, close, high, low, volume, turnover]
                        if len(candle) < 6:
                            continue
                        timestamp = int(float(candle[0]))
                        open_price = float(candle[1])
                        high_price = float(candle[3])
                        low_price = float(candle[4])
                        close_price = float(candle[2])
                        volume = float(candle[5])

                    else:
                        continue

                    normalized_data['timestamp'].append(timestamp)
                    normalized_data['open'].append(open_price)
                    normalized_data['high'].append(high_price)
                    normalized_data['low'].append(low_price)
                    normalized_data['close'].append(close_price)
                    normalized_data['volume'].append(volume)

                except Exception as e:
                    continue

            return normalized_data if len(normalized_data['timestamp']) > 0 else None

        except Exception as e:
            print(f"❌ Normalize error: {e}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """Simplified fallback: BingX Spot → KuCoin"""
        print(f"\n🔍 Fetching {symbol} {timeframe}...")

        # 1. Try BingX Spot
        data = self.fetch_bingx_spot_data(symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'BingX Spot'

        # 2. Try KuCoin
        data = self.fetch_kucoin_data(symbol, timeframe, limit)
        if data and len(data.get('timestamp', [])) > 0:
            return data, 'KuCoin'

        print(f"❌ All exchanges failed for {symbol}")
        return None, None
