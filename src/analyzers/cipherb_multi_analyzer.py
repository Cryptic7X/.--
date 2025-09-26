#!/usr/bin/env python3
"""
CipherB Multi-Timeframe Analyzer - 2H + 8H Intelligent System
PRESERVES YOUR EXACT PINE SCRIPT LOGIC - Just adds smart caching
"""
import os
import json
import sys
import pandas as pd
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.cipherb_multi import CipherBMultiIndicator
from src.alerts.cipherb_multi_telegram import CipherBMultiTelegramSender

class CipherBMultiAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.cipherb_indicator = CipherBMultiIndicator()
        self.telegram_sender = CipherBMultiTelegramSender(config)

    def load_cipherb_dataset(self) -> List[Dict]:
        """Load CipherB coin dataset"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        with open(cache_file, 'r') as f:
            data = json.load(f)
            coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} CipherB coins from cache")
        return coins

    def analyze_coin_2h(self, coin_data: Dict) -> Optional[Dict]:
        """Analyze 2H timeframe for single coin - YOUR EXACT LOGIC"""
        symbol = coin_data['symbol']
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )

            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 25:
                return None

            # DataFrame for your indicator (unchanged)
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df.astype(float).ffill().bfill()

            if len(df) < 25:
                return None

            # Analyze using your exact logic
            signal_2h = self.cipherb_indicator.analyze_timeframe(df, '2h')
            if not signal_2h:
                return None

            # Determine alert type based on cache
            should_alert, alert_type, message = self.cipherb_indicator.determine_alert_type(symbol, signal_2h)

            if should_alert:
                return {
                    'symbol': symbol,
                    'signal_type': signal_2h['signal_type'],
                    'alert_type': alert_type,
                    'wt1': signal_2h['wt1'],
                    'wt2': signal_2h['wt2'],
                    'coin_data': coin_data,
                    'exchange_used': exchange_used,
                    'message': message,
                    'timeframe': '2h'
                }

            return None

        except Exception as e:
            return None

    def analyze_coin_8h(self, symbol: str, coin_data: Dict) -> Optional[Dict]:
        """Analyze 8H timeframe for specific coin - YOUR EXACT LOGIC"""
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '8h', limit=200
            )

            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 25:
                return None

            # DataFrame for your indicator (unchanged)
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df.astype(float).ffill().bfill()

            if len(df) < 25:
                return None

            # Analyze using your exact logic
            signal_8h = self.cipherb_indicator.analyze_timeframe(df, '8h')
            if not signal_8h:
                return None

            # Check if 8h confirms 2h monitoring
            confirmed, message = self.cipherb_indicator.check_8h_confirmation(symbol, signal_8h)
            
            if confirmed:
                return {
                    'symbol': symbol,
                    'signal_type': signal_8h['signal_type'],
                    'alert_type': '2H+8H CONFIRMED',
                    'wt1': signal_8h['wt1'],
                    'wt2': signal_8h['wt2'],
                    'coin_data': coin_data,
                    'message': message,
                    'timeframe': '8h'
                }

            return None

        except Exception as e:
            return None

    def run_analysis(self):
        """Main analysis - 2H for all coins, then 8H for monitoring coins"""
        print("🎯 CIPHERB MULTI-TIMEFRAME ANALYSIS")
        print("="*50)
        start_time = datetime.utcnow()

        # Step 1: Load coins
        coins = self.load_cipherb_dataset()
        if not coins:
            return

        print(f"📊 Analyzing {len(coins)} coins on 2H timeframe")

        # Step 2: Analyze 2H for ALL coins
        signals_2h = []
        max_workers = self.config.get('cipherb', {}).get('max_workers', 10)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {
                executor.submit(self.analyze_coin_2h, coin): coin
                for coin in coins
            }

            for future in concurrent.futures.as_completed(future_to_coin):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals_2h.append(result)
                        print(f"✅ 2H {result['alert_type']}: {result['symbol']} {result['signal_type']}")
                except:
                    continue

        # Step 3: Check if any coins need 8H monitoring
        monitoring_coins = self.cipherb_indicator.get_monitoring_coins()
        signals_8h = []

        if monitoring_coins:
            print(f"📊 Analyzing {len(monitoring_coins)} coins on 8H for confirmation")
            
            # Get coin data for monitoring coins
            monitoring_coin_data = {coin['symbol']: coin for coin in coins 
                                  if coin['symbol'] in monitoring_coins}
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_symbol = {
                    executor.submit(self.analyze_coin_8h, symbol, monitoring_coin_data.get(symbol)): symbol
                    for symbol in monitoring_coins.keys()
                    if symbol in monitoring_coin_data
                }

                for future in concurrent.futures.as_completed(future_to_symbol):
                    try:
                        result = future.result(timeout=30)
                        if result:
                            signals_8h.append(result)
                            print(f"✅ 8H CONFIRMED: {result['symbol']} {result['signal_type']}")
                    except:
                        continue

        # Step 4: Send alerts
        all_signals = signals_2h + signals_8h
        if all_signals:
            success = self.telegram_sender.send_multi_alerts(all_signals)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print("\n" + "="*50)
            print("✅ CIPHERB MULTI-TIMEFRAME COMPLETE")
            print(f"🎯 2H Signals: {len(signals_2h)}")
            print(f"🎯 8H Confirmations: {len(signals_8h)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print("="*50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No CipherB signals in this cycle")
            print(f"📊 Coins in 8H monitoring: {len(monitoring_coins)}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = CipherBMultiAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
