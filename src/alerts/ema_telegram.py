#!/usr/bin/env python3
"""
EMA 15M TEST ANALYZER - Simplified with Better Error Handling
"""
import os
import json
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.ema import EMAIndicator
from src.alerts.ema_telegram import EMATelegramSender

class EMAAnalyzer15MTest:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = EMATelegramSender(config)
        self.ema_indicator = EMAIndicator()
        
        # TESTING: Reduce cooldown
        self.ema_indicator.crossover_cooldown_hours = 2

    def load_coins(self):
        """Load top 10 coins for testing"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Get top 10 only for faster testing
            filtered = [
                coin for coin in coins
                if coin.get('market_cap', 0) >= 5_000_000_000  # ≥$5B for testing
            ][:10]  # Limit to top 10
            
            print(f"📊 Loaded {len(filtered)} coins for EMA 15M testing")
            return filtered
            
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, coin_data):
        """Analyze single coin - simplified"""
        symbol = coin_data['symbol']
        
        try:
            # Get 15M OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '15m', limit=100
            )
            
            if not ohlcv_data:
                return None
            
            print(f"✅ {symbol} 15m data from {exchange_used}")
            
            # Run EMA analysis
            result = self.ema_indicator.analyze(ohlcv_data, symbol)
            
            if not (result.get('crossover_alert', False) or result.get('zone_alert', False)):
                return None

            print(f"✅ ALERT: {symbol}")
            
            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                'zone_alert': result.get('zone_alert', False),
                'zone_type': result.get('zone_type'),
                'ema21': result.get('ema21', 0),
                'ema50': result.get('ema50', 0),
                'current_price': result.get('current_price', 0),
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }

        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        """Main analysis runner - simplified"""
        print("🟡 EMA 15M SIMPLE TEST")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        # Process coins
        signals = []
        for coin in coins[:5]:  # Process only first 5 for testing
            try:
                result = self.analyze_coin(coin)
                if result:
                    signals.append(result)
            except Exception as e:
                print(f"❌ Error with {coin['symbol']}: {e}")
                continue
        
        print(f"\n📊 Found {len(signals)} signals")
        
        # Send alerts if any
        if signals:
            print(f"📱 SENDING TELEGRAM TEST...")
            success = self.telegram_sender.send_ema_alerts(signals, timeframe_minutes=15)
            print(f"📤 Result: {'✅ Success' if success else '❌ Failed'}")
        else:
            print("📭 No signals to send")

def main():
    import yaml
    try:
        with open('config/config.yaml') as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    analyzer = EMAAnalyzer15MTest(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
