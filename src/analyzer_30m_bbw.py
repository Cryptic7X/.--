#!/usr/bin/env python3
"""
BBW 30-Minute Squeeze Analyzer - OPTIMIZED VERSION
Fixed timeout issues with concurrent processing and smart batching
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
import asyncio
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

class OptimizedBBW30mAnalyzer:
    def __init__(self):
        self.start_time = time.time()
        self.max_execution_time = 12 * 60  # 12 minutes max (safe buffer)
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
        """Load and LIMIT market data for time constraint"""
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
        
        # CRITICAL: Limit to manageable size for 12-minute window
        max_coins = 100  # Reduced from 500 to ensure completion
        limited_coins = filtered_coins[:max_coins]
        
        print(f"📊 Market data: {len(coins)} total → {len(limited_coins)} for analysis")
        return limited_coins
    
    def init_exchanges(self):
        """Initialize exchange connections - PRIMARY ONLY for speed"""
        exchanges = []
        
        # ONLY BingX for speed - no fallbacks during time constraint
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 200,  # Reduced from 300
                'enableRateLimit': True,
                'timeout': 10000,  # Reduced timeout
            })
            exchanges.append(('BingX', bingx))
            print("✅ BingX initialized (PRIMARY ONLY)")
        except Exception as e:
            print(f"❌ BingX failed: {e}")
            
        return exchanges
    
    def check_time_remaining(self):
        """Check if we have time remaining"""
        elapsed = time.time() - self.start_time
        remaining = self.max_execution_time - elapsed
        return remaining > 60  # Need at least 1 minute buffer
    
    def fetch_ohlcv_data_fast(self, symbol, timeframe='30m'):
        """OPTIMIZED: Fast data fetching with minimal candles"""
        if not self.exchanges:
            return None, None
            
        exchange_name, exchange = self.exchanges[0]  # Use primary only
        
        try:
            # REDUCED candles for speed: minimum required for BBW
            ohlcv = exchange.fetch_ohlcv(f"{symbol}USDT", timeframe, limit=130)
            
            if len(ohlcv) < 125:
                return None, None
                
            # Quick DataFrame creation
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Keep UTC for freshness, convert display to IST
            df['utc_timestamp'] = df.index
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            
            return df, exchange_name
            
        except Exception:
            return None, None
    
    def analyze_coin_bbw_fast(self, coin_data):
        """OPTIMIZED: Fast BBW analysis"""
        symbol = coin_data.get('symbol', '').upper()
        
        if symbol in self.blocked_coins:
            return None
            
        try:
            # Fast data fetch
            df, exchange_used = self.fetch_ohlcv_data_fast(symbol)
            if df is None:
                return None
            
            # Fast BBW calculation
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
            
            # Quick freshness check
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
    
    def process_coins_concurrent(self, coins, max_workers=10):
        """CONCURRENT processing for speed"""
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_coin = {
                executor.submit(self.analyze_coin_bbw_fast, coin): coin 
                for coin in coins
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_coin, timeout=600):
                if not self.check_time_remaining():
                    print("⏰ TIME LIMIT APPROACHING - STOPPING EARLY")
                    break
                    
                try:
                    result = future.result()
                    if result:
                        signals.append(result)
                        print(f"🚨 {result['symbol']} BBW SQUEEZE")
                except Exception as e:
                    continue
                    
        return signals
    
    def run_optimized_bbw_analysis(self):
        """OPTIMIZED: Fast BBW analysis with time management"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🚀 OPTIMIZED BBW 30-MINUTE ANALYSIS")
        print("=" * 80)
        print(f"📅 Start Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"⏱️ Max Execution: {self.max_execution_time//60} minutes")
        print(f"📊 Coins to analyze: {len(self.market_data)} (optimized)")
        print(f"🚀 Processing: CONCURRENT (10 workers)")
        
        if not self.market_data:
            print("❌ No market data")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # CONCURRENT processing in small batches
        detected_signals = []
        batch_size = 50  # Process in batches to manage memory
        
        for i in range(0, len(self.market_data), batch_size):
            if not self.check_time_remaining():
                print("⏰ TIME LIMIT REACHED - STOPPING")
                break
                
            batch = self.market_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} coins)")
            
            # Process batch concurrently
            batch_signals = self.process_coins_concurrent(batch)
            detected_signals.extend(batch_signals)
            
            print(f"✅ Batch {batch_num}: {len(batch_signals)} signals found")
        
        # Send alerts
        alerts_sent = 0
        if detected_signals:
            success = self.telegram.send_consolidated_bbw_alert(detected_signals)
            if success:
                alerts_sent = 1
                print(f"📱 ALERT SENT: {len(detected_signals)} BBW signals")
        
        # Final summary
        elapsed = (time.time() - self.start_time) / 60
        print("=" * 80)
        print("✅ OPTIMIZED BBW ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"⏱️ Execution time: {elapsed:.1f} minutes")
        print(f"📊 Coins processed: ~{len(self.market_data)}")
        print(f"🚨 BBW signals: {len(detected_signals)}")
        print(f"📱 Alerts sent: {alerts_sent}")
        print("=" * 80)

if __name__ == "__main__":
    analyzer = OptimizedBBW30mAnalyzer()
    analyzer.run_optimized_bbw_analysis()
