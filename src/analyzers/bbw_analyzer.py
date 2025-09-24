#!/usr/bin/env python3
"""Simple BBW 15M Analyzer"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.bbw import BBWIndicator
from src.alerts.bbw_telegram import BBWTelegramSender

class SimpleBBWAnalyzer:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator()

    def load_dataset(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
                
            # Filter for BBW requirements (≥$100M cap, ≥$50M vol)
            filtered = [
                coin for coin in coins 
                if coin.get('market_cap', 0) >= 100_000_000 and coin.get('total_volume', 0) >= 50_000_000
            ]
            
            return filtered
        except:
            return []

    def analyze_coin(self, coin_data):
        symbol = coin_data['symbol']
        
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '15m', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            bbw_result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            if not bbw_result.get('squeeze_signal', False):
                return None

            return {
                'symbol': symbol,
                'signal_type': 'BBW_SQUEEZE',
                'bbw_value': bbw_result.get('bbw', 0),
                'contraction_line': bbw_result.get('lowest_contraction', 0),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '15m'
            }

        except Exception as e:
            print(f"Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        print("🔵 Starting BBW 15M Analysis...")
        
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
                        print(f"🔵 BBW SQUEEZE: {result['symbol']}")
                except:
                    continue
        
        if signals:
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            print(f"✅ Found {len(signals)} signals, Alert sent: {success}")
        else:
            print("📭 No BBW signals found")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = SimpleBBWAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
