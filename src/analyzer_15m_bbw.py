#!/usr/bin/env python3
"""
Enhanced 15-Minute BBW Analyzer - Production Ready (FIXED)
Comprehensive squeeze detection with diagnostics and error recovery
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from alerts.telegram_batch import send_consolidated_alert
from alerts.deduplication_15m import BBW15mDeduplicator
from indicators.bbw_exact import detect_exact_bbw_signals, get_latest_squeeze_signal, validate_bbw_data

def get_ist_time():
    """Convert UTC to IST"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

class EnhancedBBW15mAnalyzer:
    def __init__(self):
        # CRITICAL FIX: Initialize processing_stats FIRST
        self.processing_stats = {
            'total_coins_loaded': 0,
            'blocked_coins': 0,
            'data_fetch_failed': 0,
            'insufficient_candles': 0,
            'bbw_calc_failed': 0,
            'no_signals': 0,
            'signals_found': 0,
            'processing_errors': 0,
            'alerts_sent': 0
        }
        
        # Now initialize other components
        self.config = self.load_config()
        self.deduplicator = BBW15mDeduplicator(
            freshness_minutes=self.config.get('freshness_window', 20)
        )
        self.exchanges = self.init_enhanced_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data_with_verification()
    
    def load_config(self):
        """Load enhanced configuration with fallbacks"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
                print(f"✅ Configuration loaded from {config_path}")
                return config
        except Exception as e:
            print(f"❌ Config load error: {e}")
            return {
                'bbw': {'squeeze_tolerance': 0.05, 'length': 20, 'multiplier': 2.0, 'contraction_length': 125},
                'freshness_window': 20,
                'exchanges': {'retry_attempts': 3, 'timeout_seconds': 45}
            }
    
    def load_blocked_coins(self):
        """Load blocked coins with enhanced error handling"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins")
            if blocked_coins and len(blocked_coins) <= 15:
                print(f"   Blocked: {', '.join(sorted(blocked_coins))}")
            elif len(blocked_coins) > 15:
                sample = sorted(list(blocked_coins))[:15]
                print(f"   Sample: {', '.join(sample)} (+{len(blocked_coins)-15} more)")
                
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data_with_verification(self):
        """Enhanced market data loading with cache verification"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        print("\n" + "=" * 60)
        print("📊 ENHANCED MARKET DATA LOADING")
        print("=" * 60)
        
        if not os.path.exists(cache_file):
            print("❌ Cache file not found")
            return []
        
        # Check cache file metadata
        cache_age = time.time() - os.path.getmtime(cache_file)
        cache_hours = cache_age / 3600
        
        print(f"📁 Cache file age: {cache_hours:.1f} hours")
        
        if cache_hours > 24:
            print(f"⚠️ Cache is {cache_hours:.1f} hours old - consider refreshing")
        
        try:
            with open(cache_file) as f:
                data = json.load(f)
            
            all_coins = data.get('coins', [])
            total_from_metadata = data.get('total_coins', len(all_coins))
            
            print(f"📊 Cache metadata reports: {total_from_metadata} coins")
            print(f"📊 Actually loaded: {len(all_coins)} coins")
            
            if len(all_coins) != total_from_metadata:
                print(f"⚠️ CACHE INCONSISTENCY DETECTED!")
                print(f"   Metadata: {total_from_metadata}")
                print(f"   Actual: {len(all_coins)}")
            
            # Enhanced filtering with detailed tracking
            major_coins = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOT', 'MATIC', 'LINK', 'AVAX', 'UNI']
            
            priority_coins = []
            regular_coins = []
            blocked_count = 0
            invalid_count = 0
            
            for coin in all_coins:
                symbol = coin.get('symbol', '').upper()
                
                # Check blocked coins
                if symbol in self.blocked_coins:
                    blocked_count += 1
                    continue
                
                # Validate required fields
                if not self.validate_coin_data(coin):
                    invalid_count += 1
                    continue
                
                # Categorize coins
                if symbol in major_coins:
                    priority_coins.append(coin)
                else:
                    regular_coins.append(coin)
            
            # Sort and combine
            priority_coins.sort(key=lambda x: x.get('market_cap', 0), reverse=True)
            filtered_coins = priority_coins + regular_coins
            
            # Enhanced reporting
            print(f"\n📊 FILTERING RESULTS:")
            print(f"   Raw coins: {len(all_coins)}")
            print(f"   Blocked: {blocked_count}")
            print(f"   Invalid data: {invalid_count}")
            print(f"   Valid coins: {len(filtered_coins)}")
            print(f"   Priority coins: {len(priority_coins)}")
            
            # Show sample coins
            if filtered_coins:
                print(f"\n📋 Sample coins (first 10):")
                for coin in filtered_coins[:10]:
                    symbol = coin.get('symbol', 'N/A')
                    price = coin.get('current_price', 0)
                    mc = coin.get('market_cap', 0) / 1000000
                    print(f"   {symbol}: ${price:.4f} (MC: {mc:.0f}M)")
            
            print("=" * 60)
            
            # NOW we can safely access self.processing_stats
            self.processing_stats['total_coins_loaded'] = len(filtered_coins)
            return filtered_coins
            
        except Exception as e:
            print(f"❌ Error loading market data: {e}")
            return []
    
    def validate_coin_data(self, coin):
        """Validate individual coin data quality"""
        required_fields = ['symbol', 'current_price', 'market_cap', 'price_change_percentage_24h']
        
        for field in required_fields:
            if field not in coin or coin[field] is None:
                return False
        
        # Check for reasonable values
        if coin.get('current_price', 0) <= 0:
            return False
        if coin.get('market_cap', 0) <= 0:
            return False
            
        return True
    
    def init_enhanced_exchanges(self):
        """Initialize multiple exchanges with enhanced configuration"""
        exchanges = []
        
        # Enhanced exchange configurations
        exchange_configs = [
            {
                'name': 'BingX',
                'class': ccxt.bingx,
                'config': {
                    'apiKey': os.getenv('BINGX_API_KEY', ''),
                    'secret': os.getenv('BINGX_SECRET_KEY', ''),
                    'rateLimit': 300,
                    'enableRateLimit': True,
                    'timeout': 45000,
                }
            },
            {
                'name': 'KuCoin',
                'class': ccxt.kucoin,
                'config': {
                    'rateLimit': 500,
                    'enableRateLimit': True,
                    'timeout': 45000,
                }
            },
            {
                'name': 'Binance',
                'class': ccxt.binance,
                'config': {
                    'rateLimit': 1200,
                    'enableRateLimit': True,
                    'timeout': 45000,
                }
            }
        ]
        
        for exchange_config in exchange_configs:
            try:
                exchange = exchange_config['class'](exchange_config['config'])
                exchanges.append((exchange_config['name'], exchange))
                print(f"✅ {exchange_config['name']} initialized")
            except Exception as e:
                print(f"⚠️ {exchange_config['name']} initialization failed: {e}")
        
        if not exchanges:
            print("❌ No exchanges initialized - system cannot function")
        
        return exchanges
    
    def fetch_15m_ohlcv_enhanced(self, symbol):
        """Enhanced 15m OHLCV fetching with retry logic"""
        
        max_retries = 3
        backoff_multiplier = 2
        
        for exchange_name, exchange in self.exchanges:
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=140)
                    
                    if len(ohlcv) < 125:
                        if attempt == 0:
                            print(f"   {symbol}: Only {len(ohlcv)} candles on {exchange_name}")
                        break
                    
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    
                    df['utc_timestamp'] = df.index
                    df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                    
                    if len(df) >= 125 and df['close'].iloc[-1] > 0:
                        print(f"✅ {symbol}: Success on {exchange_name} ({len(df)} candles)")
                        return df, exchange_name
                    
                except Exception as e:
                    error_msg = str(e)[:50]
                    if attempt < max_retries - 1:
                        wait_time = backoff_multiplier ** attempt
                        print(f"   {symbol}: {exchange_name} attempt {attempt + 1} failed, retry in {wait_time}s")
                        time.sleep(wait_time)
                    else:
                        print(f"   {symbol}: {exchange_name} all attempts failed: {error_msg}")
        
        return None, None
    
    def analyze_coin_comprehensive(self, coin_data, coin_number):
        """Comprehensive coin analysis with detailed logging"""
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Stage 1: Check if blocked
            if symbol in self.blocked_coins:
                self.processing_stats['blocked_coins'] += 1
                return None
            
            # Stage 2: Fetch enhanced OHLCV data
            price_df, exchange_used = self.fetch_15m_ohlcv_enhanced(symbol)
            
            if price_df is None:
                self.processing_stats['data_fetch_failed'] += 1
                return None
            
            # Stage 3: Calculate BBW
            signals_df = detect_exact_bbw_signals(price_df, self.config)
            
            if signals_df.empty:
                self.processing_stats['bbw_calc_failed'] += 1
                return None
            
            # Stage 4: Check for squeeze signals
            latest_signal = get_latest_squeeze_signal(signals_df)
            
            if not latest_signal:
                self.processing_stats['no_signals'] += 1
                return None
            
            # Stage 5: Check freshness
            signal_timestamp_utc = price_df['utc_timestamp'].iloc[-1]
            
            if not self.deduplicator.is_signal_fresh_and_new(symbol, signal_timestamp_utc):
                return None
            
            # SUCCESS
            self.processing_stats['signals_found'] += 1
            squeeze_type = latest_signal.get('squeeze_type', 'NORMAL')
            
            print(f"🔥 #{coin_number}: {symbol} - {squeeze_type} SQUEEZE! (Strength: {latest_signal['squeeze_strength']:.6f})")
            
            return {
                'symbol': symbol,
                'price': coin_data.get('current_price', 0),
                'change_24h': coin_data.get('price_change_percentage_24h', 0),
                'market_cap': coin_data.get('market_cap', 0),
                'exchange': exchange_used,
                'bbw_value': latest_signal['bbw_value'],
                'lowest_contraction': latest_signal['lowest_contraction'],
                'squeeze_strength': latest_signal['squeeze_strength'],
                'squeeze_type': squeeze_type,
                'timestamp': signals_df.index[-1],
                'coin_data': coin_data
            }
            
        except Exception as e:
            self.processing_stats['processing_errors'] += 1
            print(f"💥 #{coin_number}: {symbol} - ERROR: {str(e)[:100]}")
            return None
    
    def run_enhanced_analysis(self):
        """Run comprehensive 15m BBW analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🚀 ENHANCED 15M BBW ANALYSIS - PRODUCTION MODE")
        print("=" * 80)
        print(f"🕐 Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"🎯 Tolerance: {self.config.get('bbw', {}).get('squeeze_tolerance', 0.05)}")
        print(f"⏰ Freshness: {self.config.get('freshness_window', 20)} minutes")
        print(f"🔍 Coins to analyze: {len(self.market_data)}")
        
        if not self.market_data:
            print("❌ No market data available for analysis")
            return
        
        # Reset and cleanup
        self.deduplicator.cleanup_old_signals()
        
        # Process all coins
        squeeze_signals = []
        
        print(f"\n🔄 STARTING COMPREHENSIVE ANALYSIS")
        print("=" * 50)
        
        for i, coin in enumerate(self.market_data, 1):
            if i <= 10 or i % 20 == 0 or i == len(self.market_data):
                print(f"📊 Processing coin {i}/{len(self.market_data)}")
            
            signal_result = self.analyze_coin_comprehensive(coin, i)
            
            if signal_result:
                squeeze_signals.append(signal_result)
            
            time.sleep(0.3)
        
        # Generate final report
        self.generate_final_report(squeeze_signals)
        
        # Send alerts
        if squeeze_signals:
            try:
                success = send_consolidated_alert(squeeze_signals)
                if success:
                    self.processing_stats['alerts_sent'] = 1
                    print(f"\n✅ ALERT SENT: {len(squeeze_signals)} squeeze signals")
                else:
                    print(f"\n❌ ALERT FAILED: Could not send {len(squeeze_signals)} signals")
            except Exception as e:
                print(f"\n❌ ALERT ERROR: {e}")
        else:
            print(f"\n📭 NO ALERTS: No squeeze signals detected")
        
        print("=" * 80)
    
    def generate_final_report(self, signals):
        """Generate comprehensive final analysis report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE ANALYSIS REPORT")
        print("=" * 60)
        
        total_processed = len(self.market_data)
        
        # Processing statistics
        print(f"📈 PROCESSING BREAKDOWN:")
        for category, count in self.processing_stats.items():
            if total_processed > 0:
                percentage = (count / total_processed) * 100
                category_name = category.replace('_', ' ').title()
                print(f"   {category_name}: {count} ({percentage:.1f}%)")
        
        # Summary
        success_rate = (self.processing_stats['signals_found'] / total_processed * 100) if total_processed > 0 else 0
        print(f"\n📊 SUMMARY:")
        print(f"   Total Processed: {total_processed}")
        print(f"   Success Rate: {self.processing_stats['signals_found']}/{total_processed} ({success_rate:.1f}%)")
        print(f"   Alerts Sent: {self.processing_stats['alerts_sent']}")

if __name__ == '__main__':
    analyzer = EnhancedBBW15mAnalyzer()
    analyzer.run_enhanced_analysis()
