#!/usr/bin/env python3
"""
BBW 30-Minute Squeeze Analyzer - FIXED VERSION
Fixed BingX symbol formatting issue
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
import concurrent.futures
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from indicators.bbw_pinescript_exact import BBWIndicator
from alerts.telegram_coinglass import TelegramCoinGlassAlert
from alerts.deduplication_30m import BBWDeduplicator

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class FixedBBW30mAnalyzer:
    def __init__(self):
        self.start_time = time.time()
        self.max_execution_time = 12 * 60  # 12 minutes max
        self.config = self.load_config()
        self.deduplicator = BBWDeduplicator()
        self.telegram = TelegramCoinGlassAlert()
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data()
        
    def load_config(self):
        """Load system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        """Load blocked coins from file"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            print(f"📛 Loaded {len(blocked_coins)} blocked coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data(self):
        """Load and filter market data"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_12h.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
            
        with open(cache_file) as f:
            data = json.load(f)
            
        coins = data.get('coins', [])
        
        # Filter out blocked coins
        filtered_coins = [coin for coin in coins 
                         if coin.get('symbol', '').upper() not in self.blocked_coins]
        
        # Apply quality filters
        quality_filtered = []
        for coin in filtered_coins:
            market_cap = coin.get('market_cap', 0) or 0
            volume_24h = coin.get('total_volume', 0) or 0
            
            if market_cap >= 50_000_000 and volume_24h >= 10_000_000:
                quality_filtered.append(coin)
        
        # Limit for performance - top 100 coins
        final_coins = quality_filtered[:100]
        
        print(f"📊 Market data: {len(coins)} total → {len(final_coins)} for analysis")
        return final_coins
    
    def init_exchanges(self):
        """Initialize BingX exchange"""
        exchanges = []
        
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 20000,
            })
            
            # Test connection
            bingx.load_markets()
            exchanges.append(('BingX', bingx))
            print("✅ BingX initialized successfully")
            
        except Exception as e:
            print(f"❌ BingX initialization failed: {e}")
            
        return exchanges
    
    def check_time_remaining(self):
        """Check if we have time remaining"""
        elapsed = time.time() - self.start_time
        remaining = self.max_execution_time - elapsed
        return remaining > 60
    
    def fetch_ohlcv_data_fixed(self, symbol, timeframe='30m'):
        """FIXED: Correct symbol formatting for BingX"""
        if not self.exchanges:
            return None, None
            
        exchange_name, exchange = self.exchanges[0]
        
        try:
            # FIXED: Use correct BingX symbol format with slash
            symbol_formatted = f"{symbol}/USDT"  # BTC/USDT not BTCUSDT
            
            ohlcv = exchange.fetch_ohlcv(symbol_formatted, timeframe, limit=130)
            
            if len(ohlcv) < 125:
                return None, None
                
            # Convert to DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Keep UTC timestamps for freshness checking
            df['utc_timestamp'] = df.index
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            
            return df, exchange_name
            
        except Exception:
            return None, None
    
    def analyze_coin_bbw_fixed(self, coin_data):
        """FIXED: BBW analysis with correct symbol handling"""
        symbol = coin_data.get('symbol', '').upper()
        
        if symbol in self.blocked_coins:
            return None
            
        try:
            # Fetch data with fixed symbol formatting
            df, exchange_used = self.fetch_ohlcv_data_fixed(symbol)
            if df is None:
                return None
            
            # BBW calculation
            bbw_indicator = BBWIndicator(
                length=self.config['bbw']['length'],
                multiplier=self.config['bbw']['multiplier'],
                contraction_length=self.config['bbw']['contraction_length']
            )
            
            signals_df = bbw_indicator.calculate_bbw_signals(df)
            if signals_df.empty:
                return None
            
            # Get latest signal
            latest_signal = bbw_indicator.get_latest_squeeze_signal(
                signals_df,
                tolerance=self.config['bbw']['squeeze_tolerance'],
                separation_threshold=self.config['bbw']['separation_threshold']
            )
            
            if not latest_signal:
                return None
            
            # Check freshness
            signal_timestamp = latest_signal['timestamp']
            utc_timestamp = df[df.index == signal_timestamp]['utc_timestamp'].iloc[0]
            
            if not self.deduplicator.is_signal_fresh_and_new(symbol, utc_timestamp):
                return None
            
            return {
                'symbol': symbol,
                'price': coin_data.get('current_price', 0),
                'change_24h': coin_data.get('price_change_percentage_24h', 0),
                'market_cap': coin_data.get('market_cap', 0),
                'exchange': exchange_used,
                'bbw_value': latest_signal['bbw_value'],
                'lowest_contraction': latest_signal['lowest_contraction'],
                'squeeze_strength': latest_signal.get('squeeze_strength', 'MODERATE'),
                'timestamp': signal_timestamp,
                'coin_data': coin_data
            }
            
        except Exception:
            return None
    
    def process_coins_concurrent(self, coins, max_workers=8):
        """Process coins concurrently"""
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {
                executor.submit(self.analyze_coin_bbw_fixed, coin): coin 
                for coin in coins
            }
            
            for future in concurrent.futures.as_completed(future_to_coin, timeout=600):
                if not self.check_time_remaining():
                    print("⏰ TIME LIMIT APPROACHING - STOPPING EARLY")
                    break
                    
                try:
                    result = future.result()
                    if result:
                        signals.append(result)
                        print(f"🚨 BBW SQUEEZE: {result['symbol']} (Strength: {result['squeeze_strength']})")
                except Exception:
                    continue
                    
        return signals
    
    def run_fixed_bbw_analysis(self):
        """Run fixed BBW analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🚀 FIXED BBW 30-MINUTE ANALYSIS")
        print("=" * 80)
        print(f"📅 Start Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"🔧 Fix Applied: Correct BingX symbol formatting (BTC/USDT not BTCUSDT)")
        print(f"📊 Coins to analyze: {len(self.market_data)}")
        print(f"🚀 Processing: CONCURRENT (8 workers)")
        
        if not self.market_data:
            print("❌ No market data")
            return
        
        if not self.exchanges:
            print("❌ No exchanges available")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # Process in batches
        detected_signals = []
        batch_size = 50
        
        for i in range(0, len(self.market_data), batch_size):
            if not self.check_time_remaining():
                print("⏰ TIME LIMIT REACHED - STOPPING")
                break
                
            batch = self.market_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} coins)")
            
            batch_signals = self.process_coins_concurrent(batch)
            detected_signals.extend(batch_signals)
            
            print(f"✅ Batch {batch_num}: {len(batch_signals)} signals found")
        
        # Send alerts
        alerts_sent = 0
        if detected_signals:
            try:
                success = self.telegram.send_consolidated_bbw_alert(detected_signals)
                if success:
                    alerts_sent = 1
                    print(f"📱 ALERT SENT: {len(detected_signals)} BBW signals")
            except Exception as e:
                print(f"❌ Alert failed: {str(e)[:100]}")
        
        # Final summary
        elapsed = (time.time() - self.start_time) / 60
        print("=" * 80)
        print("✅ FIXED BBW ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"⏱️ Execution time: {elapsed:.1f} minutes")
        print(f"📊 Coins processed: ~{len(self.market_data)}")
        print(f"🚨 BBW signals: {len(detected_signals)}")
        print(f"📱 Alerts sent: {alerts_sent}")
        
        if detected_signals:
            print(f"🎯 Signals found:")
            for signal in detected_signals:
                print(f"   - {signal['symbol']}: BBW={signal['bbw_value']:.4f}, Strength={signal['squeeze_strength']}")
        
        print("=" * 80)

if __name__ == "__main__":
    analyzer = FixedBBW30mAnalyzer()
    analyzer.run_fixed_bbw_analysis()
