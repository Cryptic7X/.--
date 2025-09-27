#!/usr/bin/env python3
"""
EMA 4H Analyzer - FIXED VERSION
"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.ema import EMAIndicator
from src.alerts.ema_telegram import EMATelegramSender

class EMAAnalyzer:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = EMATelegramSender(config)
        self.ema_indicator = EMAIndicator()

    # MODIFY the load_coins method:
    def load_coins(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            coins = data.get('coins', [])
            
            # UPDATED FILTER: Market cap $10M-$100M (inclusive), Volume >= $10M
            filtered = [
                coin for coin in coins
                if 10_000_000 <= coin.get('market_cap', 0) <= 100_000_000  # CHANGED: Upper bound added
                and coin.get('total_volume', 0) >= 10_000_000
            ]
            
            print(f"📊 Loaded {len(filtered)} coins for EMA 1H analysis")  # CHANGED: 4H -> 1H
            return filtered
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []
    
    # MODIFY the analyze_coin method:
    def analyze_coin(self, coin_data):
        symbol = coin_data['symbol']
        try:
            # CHANGED: '4h' -> '1h', limit=100 -> limit=200
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '1h', limit=200  # CHANGED: More data for accuracy
            )
            
            if not ohlcv_data:
                print(f"❌ No data fetched for {symbol}")  # ADDED: Error logging
                return None
            
            result = self.ema_indicator.analyze(ohlcv_data, symbol)
            
            # CHANGED: Only check crossover_alert, remove zone_alert check
            if not result.get('crossover_alert', False):
                return None
            
            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                # REMOVED: zone_alert and zone_type fields
                'ema21': result.get('ema21'),
                'ema50': result.get('ema50'),
                'current_price': result.get('current_price'),
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }
            
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")  # ADDED: Error logging
            return None
    
    # MODIFY the run_analysis method:
    def run_analysis(self):
        print("🟡 EMA 1H ANALYSIS - PRODUCTION")  # CHANGED: 4H -> 1H
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        
        coins = self.load_coins()
        if not coins:
            return
        
        signals = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        # CHANGED: Only crossover alerts, remove zone logic
                        alert_types = []
                        if result['crossover_alert']:
                            alert_types.append(result['crossover_type'].upper())
                        
                        print(f"✅ ALERT: {result['symbol']} ({', '.join(alert_types)})")
                except Exception as e:
                    print(f"❌ Analysis timeout/error: {e}")  # ADDED: Error logging
                    continue
        
        if signals:
            # CHANGED: timeframe_minutes=240 -> 60
            success = self.telegram_sender.send_ema_alerts(signals, timeframe_minutes=60)
            
            crossover_count = len([s for s in signals if s.get('crossover_alert')])
            # REMOVED: zone_count calculation
            
            print(f"📱 Results: {crossover_count} crossovers")  # CHANGED: Removed zone count
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No EMA crossover signals found")  # CHANGED: Added "crossover"
        
        cache = self.ema_indicator.load_ema_cache()
        print(f"📁 Final EMA cache: {len(cache)} tracked symbols")

def main():
    import yaml
    try:
        with open('config/config.yaml') as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    analyzer = EMAAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
