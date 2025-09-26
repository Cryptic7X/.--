#!/usr/bin/env python3

"""

BBW 2H Analyzer - Thread-Safe Version

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

class BBWAnalyzer:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        
        # IMPORTANT: Create ONE shared instance for thread safety
        self.bbw_indicator = BBWIndicator()
    
    def load_coins(self):
        """Load coins for analysis"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Filter: Market cap ≥ $100M, Volume ≥ $50M
            filtered = [
                coin for coin in coins 
                if coin.get('market_cap', 0) >= 100_000_000 
                and coin.get('total_volume', 0) >= 50_000_000
            ]
            
            print(f"📊 Loaded {len(filtered)} coins for BBW analysis")
            return filtered
            
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []
    
    def analyze_coin(self, coin_data):
        """Analyze single coin - uses shared thread-safe BBW indicator"""
        symbol = coin_data['symbol']
        
        try:
            # Get 2H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run BBW analysis (thread-safe)
            result = self.bbw_indicator.analyze(ohlcv_data, symbol)
            
            if not result.get('send_alert', False):
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
            
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None
    
    def run_analysis(self):
        """Main analysis runner"""
        print("🔵 BBW 2H ANALYSIS - THREAD-SAFE VERSION")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        # Process coins in parallel with thread-safe cache
        signals = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # Reduced workers to prevent race conditions
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ ALERT: {result['symbol']} ({result['alert_type']})")
                except Exception as e:
                    print(f"❌ Future error: {e}")
                    continue
        
        # Send alerts if any
        if signals:
            success = self.telegram_sender.send_bbw_alerts(signals)
            
            first_entry = len([s for s in signals if s.get('alert_type') == 'FIRST ENTRY'])
            reminders = len([s for s in signals if s.get('alert_type') == 'EXTENDED SQUEEZE'])
            
            print(f"📱 Results: {first_entry} first entries, {reminders} reminders")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No BBW squeeze alerts to send")
        
        # Print final cache status
        cache = self.bbw_indicator.load_cache()
        print(f"📁 Final cache: {len(cache)} tracked symbols")

def main():
    import yaml
    try:
        with open('config/config.yaml') as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    analyzer = BBWAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
