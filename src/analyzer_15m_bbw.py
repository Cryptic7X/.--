#!/usr/bin/env python3
"""
15-Minute BBW Analyzer with Enhanced Diagnostics
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
from indicators.bbw_exact import detect_exact_bbw_signals, get_latest_squeeze_signal

def get_ist_time():
    """Convert UTC to IST"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

class BBW15mAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.deduplicator = BBW15mDeduplicator()
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data_with_diagnostics()  # Enhanced version
        
        # Diagnostic counters
        self.processing_stats = {
            'total_loaded': 0,
            'blocked_coins': 0,
            'data_fetch_failed': 0,
            'insufficient_candles': 0,
            'bbw_calc_failed': 0,
            'no_signals': 0,
            'signals_found': 0,
            'processing_errors': 0
        }
    
    def load_config(self):
        """Load 15m BBW configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        """Load blocked coins"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins from file")
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data_with_diagnostics(self):
        """Enhanced market data loading with detailed diagnostics"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        print("\n" + "=" * 60)
        print("🔍 DETAILED MARKET DATA LOADING DIAGNOSTICS")
        print("=" * 60)
        
        if not os.path.exists(cache_file):
            print("❌ Market data cache file not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        all_coins = data.get('coins', [])
        print(f"📂 Raw coins loaded from cache: {len(all_coins)}")
        
        # Major coins for priority
        major_coins = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOT', 'MATIC', 'LINK', 'AVAX', 'UNI']
        
        # Detailed filtering with diagnostics
        priority_coins = []
        regular_coins = []
        blocked_count = 0
        invalid_data_count = 0
        missing_fields_count = 0
        
        for i, coin in enumerate(all_coins, 1):
            symbol = coin.get('symbol', '').upper()
            
            # Check for missing symbol
            if not symbol:
                missing_fields_count += 1
                print(f"⚠️  Coin #{i}: Missing symbol field")
                continue
            
            # Check blocked coins
            if symbol in self.blocked_coins:
                blocked_count += 1
                continue
            
            # Check for required fields
            required_fields = ['current_price', 'market_cap', 'price_change_percentage_24h']
            missing_field = None
            
            for field in required_fields:
                if field not in coin or coin[field] is None:
                    missing_field = field
                    break
            
            if missing_field:
                missing_fields_count += 1
                print(f"⚠️  {symbol}: Missing {missing_field}")
                continue
            
            # Check for reasonable values
            market_cap = coin.get('market_cap', 0)
            current_price = coin.get('current_price', 0)
            
            if market_cap <= 0 or current_price <= 0:
                invalid_data_count += 1
                print(f"⚠️  {symbol}: Invalid data (MC: {market_cap}, Price: {current_price})")
                continue
            
            # Valid coin - categorize
            if symbol in major_coins:
                priority_coins.append(coin)
            else:
                regular_coins.append(coin)
        
        # Sort and combine
        priority_coins.sort(key=lambda x: x.get('market_cap', 0), reverse=True)
        filtered_coins = priority_coins + regular_coins
        
        # Print detailed diagnostics
        print(f"\n📊 DETAILED FILTERING BREAKDOWN:")
        print(f"   Raw coins from cache: {len(all_coins)}")
        print(f"   Blocked coins: {blocked_count}")
        print(f"   Missing fields: {missing_fields_count}")
        print(f"   Invalid data: {invalid_data_count}")
        print(f"   Valid coins: {len(filtered_coins)}")
        print(f"   Priority coins: {len(priority_coins)}")
        print(f"   Regular coins: {len(regular_coins)}")
        
        # Show sample of filtered coins
        if len(filtered_coins) > 0:
            print(f"\n📋 Sample coins to analyze:")
            for coin in filtered_coins[:10]:
                symbol = coin.get('symbol', 'N/A')
                price = coin.get('current_price', 0)
                market_cap = coin.get('market_cap', 0) / 1000000  # In millions
                print(f"   {symbol}: ${price:.4f} (MC: {market_cap:.0f}M)")
        
        print("=" * 60)
        
        return filtered_coins
    
    def init_exchanges(self):
        """Initialize exchanges"""
        exchanges = []
        
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
        return exchanges
    
    def fetch_15m_ohlcv(self, symbol):
        """Fetch 15m OHLCV data"""
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=130)
                
                if len(ohlcv) < 125:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                df['utc_timestamp'] = df.index
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) >= 125 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception:
                continue
        
        return None, None
    
    def analyze_coin_with_diagnostics(self, coin_data, coin_number):
        """Enhanced analysis with detailed diagnostics"""
        symbol = coin_data.get('symbol', '').upper()
        
        # Stage 1: Double-check blocked (shouldn't happen but safety check)
        if symbol in self.blocked_coins:
            self.processing_stats['blocked_coins'] += 1
            print(f"🚫 #{coin_number}: {symbol} - BLOCKED")
            return None
        
        try:
            # Stage 2: Fetch OHLCV data
            price_df, exchange_used = self.fetch_15m_ohlcv(symbol)
            
            if price_df is None:
                self.processing_stats['data_fetch_failed'] += 1
                print(f"❌ #{coin_number}: {symbol} - DATA FETCH FAILED")
                return None
            
            if len(price_df) < 125:
                self.processing_stats['insufficient_candles'] += 1
                print(f"📊 #{coin_number}: {symbol} - INSUFFICIENT CANDLES ({len(price_df)})")
                return None
            
            # Stage 3: Calculate BBW
            signals_df = detect_exact_bbw_signals(price_df, self.config)
            
            if signals_df.empty:
                self.processing_stats['bbw_calc_failed'] += 1
                print(f"🧮 #{coin_number}: {symbol} - BBW CALCULATION FAILED")
                return None
            
            # Stage 4: Check for signals
            latest_signal = get_latest_squeeze_signal(signals_df)
            
            if not latest_signal:
                self.processing_stats['no_signals'] += 1
                print(f"📈 #{coin_number}: {symbol} - NO SQUEEZE SIGNALS")
                return None
            
            # Stage 5: Check freshness
            signal_timestamp_utc = price_df['utc_timestamp'].iloc[-1]
            
            if not self.deduplicator.is_signal_fresh_and_new(symbol, signal_timestamp_utc):
                print(f"⏰ #{coin_number}: {symbol} - SIGNAL NOT FRESH/DUPLICATE")
                return None
            
            # SUCCESS!
            self.processing_stats['signals_found'] += 1
            print(f"🔥 #{coin_number}: {symbol} - SQUEEZE SIGNAL FOUND! (Strength: {latest_signal['squeeze_strength']:.6f})")
            
            return {
                'symbol': symbol,
                'price': coin_data.get('current_price', 0),
                'change_24h': coin_data.get('price_change_percentage_24h', 0),
                'market_cap': coin_data.get('market_cap', 0),
                'exchange': exchange_used,
                'bbw_value': latest_signal['bbw_value'],
                'lowest_contraction': latest_signal['lowest_contraction'],
                'squeeze_strength': latest_signal['squeeze_strength'],
                'timestamp': signals_df.index[-1],
                'coin_data': coin_data
            }
            
        except Exception as e:
            self.processing_stats['processing_errors'] += 1
            print(f"💥 #{coin_number}: {symbol} - PROCESSING ERROR: {str(e)[:100]}")
            return None
    
    def run_15m_analysis(self):
        """Run complete 15m BBW analysis with diagnostics"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🔥 15-MINUTE BBW ANALYSIS WITH FULL DIAGNOSTICS")
        print("=" * 80)
        print(f"🕐 Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"🔍 Coins available for analysis: {len(self.market_data)}")
        
        if not self.market_data:
            print("❌ No market data available for analysis")
            return
        
        self.deduplicator.cleanup_old_signals()
        
        # Process each coin with detailed logging
        squeeze_signals = []
        
        print(f"\n🔄 STARTING DETAILED COIN-BY-COIN ANALYSIS")
        print("=" * 60)
        
        for i, coin in enumerate(self.market_data, 1):
            signal_result = self.analyze_coin_with_diagnostics(coin, i)
            
            if signal_result:
                squeeze_signals.append(signal_result)
            
            time.sleep(0.3)  # Rate limiting
        
        # Final statistics
        print("\n" + "=" * 60)
        print("📊 FINAL PROCESSING STATISTICS")
        print("=" * 60)
        
        total_processed = len(self.market_data)
        
        for category, count in self.processing_stats.items():
            percentage = (count / total_processed) * 100 if total_processed > 0 else 0
            category_name = category.replace('_', ' ').title()
            print(f"{category_name}: {count} ({percentage:.1f}%)")
        
        print(f"\nTotal Coins Processed: {total_processed}")
        print(f"Success Rate: {self.processing_stats['signals_found']}/{total_processed} ({(self.processing_stats['signals_found']/total_processed)*100:.1f}%)")
        
        # Send alerts
        if squeeze_signals:
            success = send_consolidated_alert(squeeze_signals)
            if success:
                print(f"\n✅ SENT 15M BBW ALERT: {len(squeeze_signals)} squeezes")
        else:
            print(f"\n📭 No alerts sent - no squeeze signals found")
        
        print("=" * 80)

if __name__ == '__main__':
    analyzer = BBW15mAnalyzer()
    analyzer.run_15m_analysis()
