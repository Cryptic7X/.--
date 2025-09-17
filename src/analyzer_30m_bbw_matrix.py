#!/usr/bin/env python3
"""
BBW 30-Minute Matrix Analyzer - Process ALL COINS in Batches
Each job processes a specific batch of coins based on BATCH_INDEX
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
from alerts.deduplication_30m import BBWDeduplicator

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

class MatrixBBWAnalyzer:
    def __init__(self):
        self.start_time = time.time()
        
        # Get matrix parameters from environment
        self.batch_index = int(os.getenv('BATCH_INDEX', '0'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.total_batches = int(os.getenv('TOTAL_BATCHES', '4'))
        
        self.config = self.load_config()
        self.deduplicator = BBWDeduplicator()
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data_batch()
        
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data_batch(self):
        """Load market data and return specific batch for this job"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_12h.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
            
        with open(cache_file) as f:
            data = json.load(f)
            
        coins = data.get('coins', [])
        
        # Filter out blocked coins and apply quality filters
        filtered_coins = []
        for coin in coins:
            symbol = coin.get('symbol', '').upper()
            if symbol not in self.blocked_coins:
                market_cap = coin.get('market_cap', 0) or 0
                volume_24h = coin.get('total_volume', 0) or 0
                
                if market_cap >= 50_000_000 and volume_24h >= 10_000_000:
                    filtered_coins.append(coin)
        
        # Calculate batch boundaries
        total_coins = len(filtered_coins)
        start_idx = self.batch_index * self.batch_size
        end_idx = min(start_idx + self.batch_size, total_coins)
        
        # Get batch for this specific job
        batch_coins = filtered_coins[start_idx:end_idx]
        
        print(f"📊 BATCH {self.batch_index + 1}/{self.total_batches}")
        print(f"   Total coins available: {total_coins}")
        print(f"   This batch range: {start_idx + 1} to {end_idx}")
        print(f"   Coins in this batch: {len(batch_coins)}")
        
        return batch_coins
    
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
                'timeout': 15000,
            })
            
            bingx.load_markets()
            exchanges.append(('BingX', bingx))
            print("✅ BingX initialized")
            
        except Exception as e:
            print(f"❌ BingX initialization failed: {e}")
            
        return exchanges
    
    def fetch_ohlcv_data(self, symbol, timeframe='30m'):
        """Fetch OHLCV data with correct BingX symbol formatting"""
        if not self.exchanges:
            return None, None
            
        exchange_name, exchange = self.exchanges[0]
        
        try:
            # CORRECT: Use BingX format with slash
            symbol_formatted = f"{symbol}/USDT"
            
            ohlcv = exchange.fetch_ohlcv(symbol_formatted, timeframe, limit=130)
            
            if len(ohlcv) < 125:
                return None, None
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Keep UTC timestamps for freshness checking
            df['utc_timestamp'] = df.index
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            
            return df, exchange_name
            
        except Exception:
            return None, None
    
    def analyze_coin_bbw(self, coin_data):
        """Analyze coin for BBW signals"""
        symbol = coin_data.get('symbol', '').upper()
        
        if symbol in self.blocked_coins:
            return None
            
        try:
            df, exchange_used = self.fetch_ohlcv_data(symbol)
            if df is None:
                return None
            
            bbw_indicator = BBWIndicator(
                length=self.config['bbw']['length'],
                multiplier=self.config['bbw']['multiplier'],
                contraction_length=self.config['bbw']['contraction_length']
            )
            
            signals_df = bbw_indicator.calculate_bbw_signals(df)
            if signals_df.empty:
                return None
            
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
                'batch_index': self.batch_index,
                'coin_data': coin_data
            }
            
        except Exception:
            return None
    
    def process_batch_concurrent(self, max_workers=8):
        """Process this batch concurrently"""
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {
                executor.submit(self.analyze_coin_bbw, coin): coin 
                for coin in self.market_data
            }
            
            for future in concurrent.futures.as_completed(future_to_coin, timeout=900):
                try:
                    result = future.result()
                    if result:
                        signals.append(result)
                        print(f"🚨 BBW SQUEEZE: {result['symbol']} (Strength: {result['squeeze_strength']})")
                except Exception:
                    continue
                    
        return signals
    
    def save_batch_results(self, signals):
        """Save batch results for combination later"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        results_file = os.path.join(cache_dir, f'bbw_batch_{self.batch_index}_{timestamp}.json')
        
        batch_data = {
            'batch_index': self.batch_index,
            'batch_size': len(self.market_data),
            'signals_found': len(signals),
            'timestamp': timestamp,
            'signals': signals
        }
        
        with open(results_file, 'w') as f:
            json.dump(batch_data, f, indent=2, default=str)
        
        print(f"💾 Batch results saved: {results_file}")
        return results_file
    
    def run_matrix_analysis(self):
        """Run matrix BBW analysis for this batch"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print(f"🚀 BBW MATRIX ANALYSIS - BATCH {self.batch_index + 1}")
        print("=" * 80)
        print(f"📅 Start Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"🎯 Processing ALL COINS via Matrix Strategy")
        print(f"📊 This batch: {len(self.market_data)} coins")
        print(f"🚀 Processing: CONCURRENT (8 workers)")
        
        if not self.market_data:
            print(f"❌ No coins in batch {self.batch_index}")
            return
        
        if not self.exchanges:
            print("❌ No exchanges available")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # Process this batch
        print(f"🔄 Processing batch {self.batch_index + 1}/{self.total_batches}")
        
        detected_signals = self.process_batch_concurrent()
        
        # Save results for combination
        if detected_signals:
            self.save_batch_results(detected_signals)
        
        # Summary
        elapsed = (time.time() - self.start_time) / 60
        print("=" * 80)
        print(f"✅ BATCH {self.batch_index + 1} COMPLETE")
        print("=" * 80)
        print(f"⏱️ Execution time: {elapsed:.1f} minutes")
        print(f"📊 Coins processed: {len(self.market_data)}")
        print(f"🚨 BBW signals: {len(detected_signals)}")
        
        if detected_signals:
            print(f"🎯 Signals found in this batch:")
            for signal in detected_signals:
                print(f"   - {signal['symbol']}: BBW={signal['bbw_value']:.4f}, Strength={signal['squeeze_strength']}")
        
        print("=" * 80)
        print(f"📤 Results will be combined by final job")

if __name__ == "__main__":
    analyzer = MatrixBBWAnalyzer()
    analyzer.run_matrix_analysis()
