#!/usr/bin/env python3
"""Simple SMA 2H Analyzer"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.sma import SMAIndicator
from src.alerts.sma_telegram import SMATelegramSender

class SimpleSMAAnalyzer:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = SMATelegramSender(config)
        self.sma_indicator = SMAIndicator()

    def load_dataset(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'sma_dataset.json')
        
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
            
            sma_result = self.sma_indicator.calculate_sma_signals(ohlcv_data, symbol)
            
            if not sma_result.get('signal_detected', False):
                return None

            return {
                'symbol': symbol,
                'signal_type': sma_result.get('signal_type', 'SMA_SIGNAL'),
                'sma_data': sma_result,
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '2h'
            }

        except Exception as e:
            print(f"Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        print("📈 Starting SMA 2H Analysis...")
        
        coins = self.load_dataset()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        print(f"📊 Analyzing {len(coins)} coins...")
        
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            future_to_coin = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(future_to_coin):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"📈 SMA SIGNAL: {result['symbol']}")
                except:
                    continue
        
        if signals:
            success = self.telegram_sender.send_sma_batch_alert(signals, {
                'total_crossovers': sum(1 for s in signals if 'crossover' in s.get('signal_type', '')),
                'total_ranging': sum(1 for s in signals if 'ranging' in s.get('signal_type', ''))
            })
            print(f"✅ Found {len(signals)} signals, Alert sent: {success}")
        else:
            print("📭 No SMA signals found")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = SimpleSMAAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
