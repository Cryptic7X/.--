#!/usr/bin/env python3
"""
BBW 2H Analyzer - New Deduplication Logic with Reminders
"""
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

    def load_coins(self):
        """Load BBW coins"""
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
        """Analyze single coin"""
        symbol = coin_data['symbol']
        
        try:
            # Get 2H data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run BBW analysis with new logic
            result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            # Check for new squeeze alerts
            if result.get('squeeze_signal', False):
                return {
                    'type': 'squeeze',
                    'symbol': symbol,
                    'bbw_value': result['bbw'],
                    'lowest_contraction': result['lowest_contraction'],
                    'squeeze_threshold': result['squeeze_threshold'],
                    'alert_type': result['alert_type'],
                    'coin_data': coin_data,
                    'exchange_used': exchange_used
                }
            
            # Check for reminder alerts
            elif result.get('reminder_signal', False):
                return {
                    'type': 'reminder',
                    'symbol': symbol,
                    'hours_in_squeeze': result['hours_in_squeeze'],
                    'coin_data': coin_data
                }

            return None

        except:
            return None

    def run_analysis(self):
        """Main analysis with new logic"""
        print("🔵 BBW 2H ANALYSIS - New Deduplication Logic")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        print(f"📊 Analyzing {len(coins)} coins...")
        
        # Process coins in parallel
        squeeze_signals = []
        reminder_signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        if result['type'] == 'squeeze':
                            squeeze_signals.append(result)
                            print(f"🔵 SQUEEZE: {result['symbol']} BBW:{result['bbw_value']:.2f}")
                        elif result['type'] == 'reminder':
                            reminder_signals.append(result)
                            print(f"⏰ REMINDER: {result['symbol']} ({result['hours_in_squeeze']}H in squeeze)")
                except:
                    continue
        
        # Send alerts
        alerts_sent = 0
        
        # Send squeeze alerts (first entry)
        if squeeze_signals:
            success = self.telegram_sender.send_bbw_batch_alert(squeeze_signals)
            if success:
                alerts_sent += 1
            print(f"✅ Squeeze alerts: {len(squeeze_signals)} signals, Sent: {'Yes' if success else 'Failed'}")
        
        # Send reminder alerts (20H+)
        if reminder_signals:
            success = self.telegram_sender.send_reminder_alert(reminder_signals)
            if success:
                alerts_sent += 1
            print(f"⏰ Reminder alerts: {len(reminder_signals)} signals, Sent: {'Yes' if success else 'Failed'}")
        
        if not squeeze_signals and not reminder_signals:
            print("📭 No BBW signals or reminders found")

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
