#!/usr/bin/env python3
"""
EMA 15M TEST ANALYZER - Fixed Format
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

class EMAAnalyzer15MTest:
    def __init__(self, config):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = EMATelegramSender(config)
        self.ema_indicator = EMAIndicator()
        
        # TESTING: Reduce cooldown to 2 hours instead of 48 hours
        self.ema_indicator.crossover_cooldown_hours = 2

    def load_coins(self):
        """Load top 20 coins for testing"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Get top 20 by market cap for testing
            filtered = [
                coin for coin in coins
                if coin.get('market_cap', 0) >= 1_000_000_000  # ≥$1B for testing
            ][:20]  # Limit to top 20
            
            print(f"📊 Loaded {len(filtered)} coins for EMA 15M testing")
            return filtered
            
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, coin_data):
        """Analyze single coin with verbose debugging"""
        symbol = coin_data['symbol']
        
        try:
            # Get 15M OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '15m', limit=100
            )
            
            if not ohlcv_data:
                print(f"❌ No data for {symbol}")
                return None
            
            print(f"✅ {symbol} 15m data from {exchange_used} ({len(ohlcv_data['close'])} candles)")
            print(f"🔍 ANALYZING {symbol}:")
            
            # Run EMA analysis with debugging
            result = self.ema_indicator.analyze(ohlcv_data, symbol)
            
            # Show EMA values and zone logic
            if 'ema21' in result and 'ema50' in result:
                ema21 = result['ema21']
                ema50 = result['ema50']
                current_price = result['current_price']
                
                print(f"   📊 21 EMA: ${ema21:.2f}")
                print(f"   📊 50 EMA: ${ema50:.2f}")
                print(f"   💰 Price: ${current_price:.2f}")
                
                # Show zone setup
                if ema21 > ema50:
                    print(f"   📈 BULLISH SETUP - Support Zone: [${ema50:.2f} - ${ema21:.2f}]")
                else:
                    print(f"   📉 BEARISH SETUP - Resistance Zone: [${ema21:.2f} - ${ema50:.2f}]")
                
                # Check zone touch
                distance = abs(current_price - ema21) / ema21 * 100
                print(f"   🎯 Distance from 21 EMA: {distance:.2f}%")
                
                if result.get('crossover_alert'):
                    print(f"   🚨 CROSSOVER ALERT: {result['crossover_type'].upper()}")
                
                if result.get('zone_alert'):
                    print(f"   🎯 ZONE TOUCH ALERT")
            
            if not (result.get('crossover_alert', False) or result.get('zone_alert', False)):
                print(f"   ✅ No alerts for {symbol}")
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
        """Main EMA 15M testing runner"""
        print("🟡 EMA 15M TEST ANALYSIS - CROSSOVERS + ZONE TOUCH")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        print("🔧 Testing Mode: 15M timeframe, 2H cooldown, Top 20 coins")
        print()
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        # Process coins sequentially for better debugging
        signals = []
        for coin in coins:
            try:
                result = self.analyze_coin(coin)
                if result:
                    signals.append(result)
                    
                    alert_types = []
                    if result['crossover_alert']:
                        alert_types.append(result['crossover_type'].upper())
                    if result['zone_alert']:
                        alert_types.append('ZONE TOUCH')
                    
                    print(f"✅ ALERT: {result['symbol']} ({', '.join(alert_types)})")
                print()  # Spacing between coins
                    
            except Exception as e:
                print(f"❌ Error with {coin['symbol']}: {e}")
                continue
        
        # Send alerts if any
        if signals:
            print(f"📱 SENDING TELEGRAM ALERTS...")
            # Pass timeframe_minutes for proper chart links and display
            success = self.telegram_sender.send_ema_alerts(signals, timeframe_minutes=15)
            
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
    
    analyzer = EMAAnalyzer15MTest(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
