#!/usr/bin/env python3
"""
BBW 2H Analyzer - 100% Range Logic with 20H Reminders
"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

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

    def load_coins(self):
        """Load BBW coins (≥$100M cap, ≥$50M vol)"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Filter for BBW
            filtered = [
                c for c in coins
                if c.get('market_cap', 0) >= 100_000_000 and c.get('total_volume', 0) >= 50_000_000
            ]
            
            return filtered
        except:
            return []

    def analyze_coin(self, coin_data):
        """Analyze single coin for BBW 100% range entry"""
        symbol = coin_data['symbol']
        
        try:
            # Get 2H data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run BBW analysis with smart alert logic
            result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            if not result.get('squeeze_signal', False):
                return None

            return {
                'symbol': symbol,
                'alert_type': result.get('alert_type'),
                'bbw_value': result['bbw'],
                'lowest_contraction': result['lowest_contraction'],
                'range_top': result['range_top'],
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }

        except:
            return None

    def run_analysis(self):
        """Main BBW analysis with smart deduplication"""
        print("🔵 BBW 2H ANALYSIS - 100% Range + 20H Reminders")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        print(f"📊 Analyzing {len(coins)} coins...")
        
        # Process coins in parallel
        signals = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        alert_type = result['alert_type']
                        print(f"🔵 {alert_type}: {result['symbol']} BBW:{result['bbw_value']:.2f}")
                except:
                    continue
        
        # Send alerts
        if signals:
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            first_entry = len([s for s in signals if s.get('alert_type') == 'FIRST ENTRY'])
            reminders = len([s for s in signals if s.get('alert_type') == 'EXTENDED SQUEEZE'])
            print(f"✅ {first_entry} squeezes, {reminders} reminders, Alert: {'Sent' if success else 'Failed'}")
        else:
            print("📭 No BBW signals found")

def main():
    import yaml
    try:
        with open('config/config.yaml') as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    analyzer = SimpleBBWAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
