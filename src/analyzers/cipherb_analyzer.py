#!/usr/bin/env python3
"""Simple CipherB 2H Analyzer"""
import os
import json
import sys
import pandas as pd
import concurrent.futures
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.cipherb import detect_exact_cipherb_signals
from src.alerts.cipherb_telegram import CipherBTelegramSender

class SimpleCipherBAnalyzer:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = CipherBTelegramSender(config)

    def load_dataset(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                return data.get('coins', [])
        except:
            return []

    def analyze_coin(self, coin_data):
        symbol = coin_data['symbol']
        
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
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

            signals_df = detect_exact_cipherb_signals(df, {
                'wt_channel_len': 9, 
                'wt_average_len': 12,
                'wt_ma_len': 3,
                'oversold_threshold': -60,
                'overbought_threshold': 60
            })
            
            if signals_df.empty:
                return None

            penultimate = signals_df.iloc[-2]
            
            if not (penultimate["buySignal"] or penultimate["sellSignal"]):
                return None

            return {
                'symbol': symbol,
                'signal_type': 'BUY' if penultimate["buySignal"] else 'SELL',
                'wt1': round(penultimate['wt1'], 1),
                'wt2': round(penultimate['wt2'], 1),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '2h'
            }

        except Exception as e:
            print(f"Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        print("🎯 Starting CipherB 2H Analysis...")
        
        coins = self.load_dataset()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        print(f"📊 Analyzing {len(coins)} coins...")
        
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_coin = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(future_to_coin):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"🎯 CipherB {result['signal_type']}: {result['symbol']}")
                except:
                    continue
        
        if signals:
            success = self.telegram_sender.send_cipherb_batch_alert(signals)
            print(f"✅ Found {len(signals)} signals, Alert sent: {success}")
        else:
            print("📭 No CipherB signals found")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = SimpleCipherBAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
