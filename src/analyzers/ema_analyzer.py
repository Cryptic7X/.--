#!/usr/bin/env python3
"""
EMA 4H ANALYZER - PRODUCTION VERSION
21/50 EMA Crossovers + Zone Touch Analysis
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
        
        # PRODUCTION: Full 48-hour cooldown (2 days)
        self.ema_indicator.crossover_cooldown_hours = 48

    def load_coins(self):
        """Load coins with SMA filtering (≥$10M cap, ≥$10M volume)"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # SMA filtering criteria from config
            filtered = [
                coin for coin in coins
                if coin.get('market_cap', 0) >= 10_000_000      # ≥$10M
                and coin.get('total_volume', 0) >= 10_000_000  # ≥$10M
            ]
            
            print(f"📊 Loaded {len(filtered)} coins for EMA 4H analysis")
            return filtered
            
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, coin_data):
        """Analyze single coin for EMA signals"""
        symbol = coin_data['symbol']
        
        try:
            # Get 4H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '4h', limit=100
            )
            
            if not ohlcv_data:
                return None
            
            # Run EMA analysis (no verbose debugging in production)
            result = self.ema_indicator.analyze(ohlcv_data, symbol)
            
            if not (result.get('crossover_alert', False) or result.get('zone_alert', False)):
                return None

            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                'zone_alert': result.get('zone_alert', False),
                'zone_type': result.get('zone_type'),
                'ema21': result.get('ema21'),
                'ema50': result.get('ema50'),
                'current_price': result.get('current_price'),
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }

        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        """Main EMA 4H production analysis"""
        print("🟡 EMA 4H ANALYSIS - PRODUCTION")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        # Process coins in parallel (production mode)
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
                        
                except Exception as e:
                    print(f"❌ Future error: {e}")
                    continue
        
        # Send alerts if any
        if signals:
            success = self.telegram_sender.send_ema_alerts(signals, timeframe_minutes=240)  # 4H = 240 minutes
            crossover_count = len([s for s in signals if s.get('crossover_alert')])
            zone_count = len([s for s in signals if s.get('zone_alert')])
            
            print(f"📱 Results: {crossover_count} crossovers, {zone_count} zone touches")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No EMA signals found")
        
        # Print final cache status
        cache = self.ema_indicator.load_cache()
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
