"""
Simple Exchange Manager - 15M and 2H Only (No 30M)
Optimized with unified error handling and logging
"""
import os
import json
import time
import requests
import yaml
from datetime import datetime
from typing import Optional, Tuple, Dict

from src.utils.logger import trading_logger
from src.utils.error_handler import with_retry, ExchangeError

class SimpleExchangeManager:
    def __init__(self):
        self.logger = trading_logger.get_logger('ExchangeManager')
        self.config = self._load_config()
        self.symbol_mapping = self._load_symbol_mapping()
        self.session = self._create_session()

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

    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Multi-Indicator-Exchange/3.1',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        })
        return session

    def _apply_symbol_mapping(self, symbol: str) -> Tuple[str, str]:
        """Apply symbol mapping and return (api_symbol, display_symbol)"""
        display_symbol = symbol.upper()
        api_symbol = self.symbol_mapping.get(display_symbol, display_symbol)
        return api_symbol, display_symbol

    @with_retry(max_attempts=3, delay=0.5)
    def _fetch_bingx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from BingX - 15M and 2H only"""
        api_key = os.getenv('BINGX_API_KEY')
        if not api_key:
            return None

        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        
        # BingX interval mapping - 15M and 2H only
        interval_map = {
            '15m': '15m',
            '2h': '2h'
        }
        
        if timeframe not in interval_map:
            raise ExchangeError(f"Unsupported timeframe for BingX: {timeframe}")

        headers = {'X-BX-APIKEY': api_key}
        params = {
            'symbol': f'{symbol}-USDT',
            'interval': interval_map[timeframe],
            'limit': limit
        }

        response = self.session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == 0 and data.get('data'):
            return self._normalize_ohlcv_data(data['data'], 'bingx')
        return None

    @with_retry(max_attempts=3, delay=0.5)
    def _fetch_kucoin_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from KuCoin - 15M and 2H only"""
        url = "https://api.kucoin.com/api/v1/market/candles"
        
        # KuCoin interval mapping - 15M and 2H only
        interval_map = {
            '15m': '15min',
            '2h': '2hour'
        }
        
        if timeframe not in interval_map:
            raise ExchangeError(f"Unsupported timeframe for KuCoin: {timeframe}")

        # Calculate time range
        end_time = int(time.time())
        if timeframe == '15m':
            start_time = end_time - (limit * 15 * 60)
        else:  # 2h
            start_time = end_time - (limit * 2 * 60 * 60)

        params = {
            'symbol': f'{symbol}-USDT',
            'type': interval_map[timeframe],
            'startAt': start_time,
            'endAt': end_time
        }

        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == '200000' and data.get('data'):
            return self._normalize_ohlcv_data(data['data'], 'kucoin')
        return None

    @with_retry(max_attempts=3, delay=0.5)
    def _fetch_okx_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[Dict]:
        """Fetch data from OKX - 15M and 2H only"""
        url = "https://www.okx.com/api/v5/market/candles"
        
        # OKX interval mapping - 15M and 2H only
        interval_map = {
            '15m': '15m',
            '2h': '2H'
        }
        
        if timeframe not in interval_map:
            raise ExchangeError(f"Unsupported timeframe for OKX: {timeframe}")

        params = {
            'instId': f'{symbol}-USDT',
            'bar': interval_map[timeframe],
            'limit': str(limit)
        }

        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == '0' and data.get('data'):
            return self._normalize_ohlcv_data(data['data'], 'okx')
        return None

    def _normalize_ohlcv_data(self, raw_data: list, exchange: str) -> Optional[Dict]:
        """Normalize OHLCV data from different exchanges"""
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
                    
                if exchange == 'bingx':
                    # BingX dictionary format
                    if isinstance(candle, dict):
                        normalized_data['timestamp'].append(int(float(candle.get('time', 0))))
                        normalized_data['open'].append(float(candle.get('open', 0)))
                        normalized_data['high'].append(float(candle.get('high', 0)))
                        normalized_data['low'].append(float(candle.get('low', 0)))
                        normalized_data['close'].append(float(candle.get('close', 0)))
                        normalized_data['volume'].append(float(candle.get('volume', 0)))
                    elif isinstance(candle, (list, tuple)) and len(candle) >= 6:
                        # BingX list format (fallback)
                        normalized_data['timestamp'].append(int(float(candle[0])))
                        normalized_data['open'].append(float(candle[1]))
                        normalized_data['high'].append(float(candle[2]))
                        normalized_data['low'].append(float(candle[3]))
                        normalized_data['close'].append(float(candle[4]))
                        normalized_data['volume'].append(float(candle[5]))
                        
                elif exchange == 'kucoin':
                    # KuCoin format: [timestamp, open, close, high, low, volume, turnover]
                    if len(candle) >= 6:
                        normalized_data['timestamp'].append(int(float(candle[0])))
                        normalized_data['open'].append(float(candle[1]))
                        normalized_data['high'].append(float(candle[3]))
                        normalized_data['low'].append(float(candle[4]))
                        normalized_data['close'].append(float(candle[2]))
                        normalized_data['volume'].append(float(candle[5]))
                        
                elif exchange == 'okx':
                    # OKX format: [timestamp, open, high, low, close, volume, volumeCcy]
                    if len(candle) >= 6:
                        normalized_data['timestamp'].append(int(float(candle[0])))
                        normalized_data['open'].append(float(candle[1]))
                        normalized_data['high'].append(float(candle[2]))
                        normalized_data['low'].append(float(candle[3]))
                        normalized_data['close'].append(float(candle[4]))
                        normalized_data['volume'].append(float(candle[5]))

            return normalized_data if normalized_data['timestamp'] else None
            
        except Exception as e:
            self.logger.error(f"Data normalization failed for {exchange}: {str(e)}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str, limit: int = 200) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Fetch OHLCV data with fallback chain: BingX → KuCoin → OKX
        """
        # Validate timeframe
        if timeframe not in ['15m', '2h']:
            raise ExchangeError(f"Unsupported timeframe: {timeframe}. Only 15m and 2h are supported.")

        api_symbol, display_symbol = self._apply_symbol_mapping(symbol)
        
        # Try BingX first
        try:
            data = self._fetch_bingx_data(api_symbol, timeframe, limit)
            if data and len(data.get('timestamp', [])) > 0:
                return data, 'BingX'
        except Exception as e:
            self.logger.debug(f"BingX failed for {symbol}: {str(e)}")
        
        # Try KuCoin
        try:
            data = self._fetch_kucoin_data(api_symbol, timeframe, limit)
            if data and len(data.get('timestamp', [])) > 0:
                return data, 'KuCoin'
        except Exception as e:
            self.logger.debug(f"KuCoin failed for {symbol}: {str(e)}")
        
        # Try OKX
        try:
            data = self._fetch_okx_data(api_symbol, timeframe, limit)
            if data and len(data.get('timestamp', [])) > 0:
                return data, 'OKX'
        except Exception as e:
            self.logger.debug(f"OKX failed for {symbol}: {str(e)}")
        
        self.logger.warning(f"All exchanges failed for {symbol} ({api_symbol}) on {timeframe}")
        return None, None
