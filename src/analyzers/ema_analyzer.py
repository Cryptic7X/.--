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

    def load_coins(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'ema_dataset.json')
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
                
                print(f"📊 Loaded {len(coins)} coins for EMA 30M analysis")
                return coins
                
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, coin_data):
        symbol = coin_data['symbol']
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '30m', limit=200  # CHANGED: 30-minute timeframe
            )
            
            if not ohlcv_data:
                return None
            
            result = self.ema_indicator.analyze(ohlcv_data, symbol)
            
            if not result.get('crossover_alert', False):  # ONLY crossover alerts
                return None
            
            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                'ema12': result.get('ema12'),
                'ema21': result.get('ema21'),
                'current_price': result.get('current_price'),
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }


        except Exception as e:
            return None

    def run_analysis(self):
        print("🟡 EMA 30M ANALYSIS - PRODUCTION")  # Updated message
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
                        
                        alert_types = []
                        if result['crossover_alert']:
                            alert_types.append(result['crossover_type'].upper())
                        if result['zone_alert']:
                            alert_types.append('ZONE TOUCH')
                        
                        print(f"✅ ALERT: {result['symbol']} ({', '.join(alert_types)})")
                        
                except:
                    continue
        
        if signals:
            success = self.telegram_sender.send_ema_alerts(signals, timeframe_minutes=30)  # 30 minutes
            crossover_count = len(signals)  # All signals are crossovers now
            
            print(f"📱 Results: {crossover_count} crossovers")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No EMA signals found")
        
        # FIXED: Call load_ema_cache instead of load_cache
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
