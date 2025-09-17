#!/usr/bin/env python3
"""
BBW 30-Minute Analyzer - DIAGNOSTIC VERSION
Detailed logging to identify why 0 signals are found
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

from indicators.bbw_pinescript_exact import BBWIndicator
from alerts.telegram_coinglass import TelegramCoinGlassAlert
from alerts.deduplication_30m import BBWDeduplicator

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

class DiagnosticBBWAnalyzer:
    def __init__(self):
        self.start_time = time.time()
        self.config = self.load_config()
        self.deduplicator = BBWDeduplicator()
        self.telegram = TelegramCoinGlassAlert()
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data()
        
        # Diagnostic counters
        self.stats = {
            'total_coins': 0,
            'blocked_skipped': 0,
            'data_fetch_failed': 0,
            'bbw_calc_failed': 0,
            'no_signals': 0,
            'stale_signals': 0,
            'fresh_signals': 0,
            'api_errors': [],
            'exchange_responses': []
        }
        
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
            print(f"📛 Loaded {len(blocked_coins)} blocked coins")
            if blocked_coins:
                print(f"   Sample blocked: {list(blocked_coins)[:5]}")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_12h.json')
        
        if not os.path.exists(cache_file):
            print(f"❌ Market data not found at: {cache_file}")
            return []
            
        with open(cache_file) as f:
            data = json.load(f)
            
        coins = data.get('coins', [])
        print(f"📊 Raw market data loaded: {len(coins)} coins")
        
        # Show sample of raw data
        if coins:
            sample_coin = coins[0]
            print(f"   Sample coin structure: {list(sample_coin.keys())}")
            print(f"   Sample coin: {sample_coin.get('symbol')} - ${sample_coin.get('current_price', 0)}")
        
        # Filter out blocked coins
        filtered_coins = []
        for coin in coins:
            symbol = coin.get('symbol', '').upper()
            if symbol not in self.blocked_coins:
                filtered_coins.append(coin)
            else:
                self.stats['blocked_skipped'] += 1
        
        # Apply market cap and volume filters
        quality_filtered = []
        for coin in filtered_coins:
            market_cap = coin.get('market_cap', 0) or 0
            volume_24h = coin.get('total_volume', 0) or 0
            
            if market_cap >= 50_000_000 and volume_24h >= 10_000_000:
                quality_filtered.append(coin)
        
        # Limit for testing
        final_coins = quality_filtered[:20]  # Test with 20 coins first
        
        print(f"📊 Filtering results:")
        print(f"   Total coins: {len(coins)}")
        print(f"   After blocked filter: {len(filtered_coins)}")
        print(f"   After quality filter: {len(quality_filtered)}")
        print(f"   Final for analysis: {len(final_coins)}")
        
        if final_coins:
            print(f"   Sample coins for analysis:")
            for i, coin in enumerate(final_coins[:5]):
                symbol = coin.get('symbol', 'N/A')
                price = coin.get('current_price', 0)
                mcap = coin.get('market_cap', 0) / 1_000_000
                print(f"      {i+1}. {symbol} - ${price:.4f} - MCap: ${mcap:.0f}M")
        
        return final_coins
    
    def init_exchanges(self):
        exchanges = []
        
        print("🔧 Initializing BingX exchange...")
        
        api_key = os.getenv('BINGX_API_KEY', '')
        secret_key = os.getenv('BINGX_SECRET_KEY', '')
        
        print(f"   API Key present: {bool(api_key)} (length: {len(api_key)})")
        print(f"   Secret Key present: {bool(secret_key)} (length: {len(secret_key)})")
        
        if not api_key or not secret_key:
            print("❌ BingX credentials missing!")
            return exchanges
            
        try:
            bingx = ccxt.bingx({
                'apiKey': api_key,
                'secret': secret_key,
                'sandbox': False,
                'rateLimit': 1000,  # 1 second for debugging
                'enableRateLimit': True,
                'timeout': 30000,
            })
            
            # Test connection
            print("   Testing BingX connection...")
            markets = bingx.load_markets()
            print(f"   ✅ BingX connected - {len(markets)} markets available")
            
            exchanges.append(('BingX', bingx))
            
        except Exception as e:
            print(f"❌ BingX initialization failed: {e}")
            self.stats['api_errors'].append(f"BingX init: {str(e)[:100]}")
            
        return exchanges
    
    def fetch_ohlcv_diagnostic(self, symbol, timeframe='30m'):
        """Diagnostic data fetching with detailed logging"""
        if not self.exchanges:
            print(f"❌ {symbol}: No exchanges available")
            return None, None
            
        exchange_name, exchange = self.exchanges[0]
        
        try:
            print(f"🔍 {symbol}: Fetching from {exchange_name}...")
            
            # Test if symbol exists
            symbol_with_usdt = f"{symbol}USDT"
            
            ohlcv = exchange.fetch_ohlcv(symbol_with_usdt, timeframe, limit=130)
            
            print(f"   📊 {symbol}: Got {len(ohlcv)} candles")
            self.stats['exchange_responses'].append(f"{symbol}: {len(ohlcv)} candles")
            
            if len(ohlcv) < 125:
                print(f"   ⚠️ {symbol}: Insufficient data ({len(ohlcv)} < 125)")
                return None, None
                
            # Convert to DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Check data quality
            latest_price = df['close'].iloc[-1]
            print(f"   ✅ {symbol}: Latest price ${latest_price:.6f}")
            
            # Keep UTC for freshness
            df['utc_timestamp'] = df.index
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            
            return df, exchange_name
            
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"   ❌ {symbol}: Fetch failed - {error_msg}")
            self.stats['api_errors'].append(f"{symbol}: {error_msg}")
            self.stats['data_fetch_failed'] += 1
            return None, None
    
    def analyze_coin_diagnostic(self, coin_data):
        """Diagnostic coin analysis with detailed logging"""
        symbol = coin_data.get('symbol', '').upper()
        self.stats['total_coins'] += 1
        
        print(f"\n🔍 ANALYZING: {symbol}")
        
        if symbol in self.blocked_coins:
            print(f"   📛 {symbol}: Blocked - skipping")
            self.stats['blocked_skipped'] += 1
            return None
            
        try:
            # Fetch data
            df, exchange_used = self.fetch_ohlcv_diagnostic(symbol)
            if df is None:
                return None
            
            print(f"   🔢 {symbol}: Calculating BBW...")
            
            # BBW calculation
            bbw_indicator = BBWIndicator(
                length=self.config['bbw']['length'],
                multiplier=self.config['bbw']['multiplier'],
                contraction_length=self.config['bbw']['contraction_length']
            )
            
            signals_df = bbw_indicator.calculate_bbw_signals(df)
            
            if signals_df.empty:
                print(f"   ❌ {symbol}: BBW calculation failed")
                self.stats['bbw_calc_failed'] += 1
                return None
            
            # Show BBW values
            latest_bbw = signals_df['bbw'].iloc[-1]
            latest_contraction = signals_df['lowest_contraction'].iloc[-1]
            
            print(f"   📊 {symbol}: BBW={latest_bbw:.6f}, Contraction={latest_contraction:.6f}")
            
            # Get latest signal
            latest_signal = bbw_indicator.get_latest_squeeze_signal(
                signals_df,
                tolerance=self.config['bbw']['squeeze_tolerance'],
                separation_threshold=self.config['bbw']['separation_threshold']
            )
            
            if not latest_signal:
                print(f"   📭 {symbol}: No squeeze signal detected")
                self.stats['no_signals'] += 1
                return None
            
            print(f"   🚨 {symbol}: SQUEEZE SIGNAL DETECTED!")
            print(f"       Signal BBW: {latest_signal['bbw_value']:.6f}")
            print(f"       Contraction: {latest_signal['lowest_contraction']:.6f}")
            print(f"       Strength: {latest_signal.get('squeeze_strength', 'N/A')}")
            
            # Check freshness
            signal_timestamp = latest_signal['timestamp']
            utc_timestamp = df[df.index == signal_timestamp]['utc_timestamp'].iloc[0]
            
            if not self.deduplicator.is_signal_fresh_and_new(symbol, utc_timestamp):
                print(f"   ⏰ {symbol}: Signal not fresh or duplicate")
                self.stats['stale_signals'] += 1
                return None
            
            print(f"   ✅ {symbol}: FRESH SQUEEZE SIGNAL!")
            self.stats['fresh_signals'] += 1
            
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
            
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"   ❌ {symbol}: Analysis failed - {error_msg}")
            self.stats['api_errors'].append(f"{symbol} analysis: {error_msg}")
            return None
    
    def run_diagnostic_analysis(self):
        """Run diagnostic BBW analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🔍 DIAGNOSTIC BBW ANALYSIS")
        print("=" * 80)
        print(f"📅 Start Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"📊 Coins to analyze: {len(self.market_data)}")
        print(f"🔧 Processing: SEQUENTIAL (diagnostic mode)")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        if not self.exchanges:
            print("❌ No exchanges available")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # Process sequentially for detailed diagnostics
        detected_signals = []
        
        for i, coin in enumerate(self.market_data):
            print(f"\n{'='*60}")
            print(f"COIN {i+1}/{len(self.market_data)}")
            
            signal_result = self.analyze_coin_diagnostic(coin)
            if signal_result:
                detected_signals.append(signal_result)
                print(f"🎯 SIGNAL ADDED: {signal_result['symbol']}")
            
            # Rate limiting
            time.sleep(1)
        
        # Final diagnostics
        print("\n" + "=" * 80)
        print("🔍 DIAGNOSTIC RESULTS")
        print("=" * 80)
        
        print(f"📊 Processing Statistics:")
        print(f"   Total coins processed: {self.stats['total_coins']}")
        print(f"   Blocked/skipped: {self.stats['blocked_skipped']}")
        print(f"   Data fetch failed: {self.stats['data_fetch_failed']}")
        print(f"   BBW calc failed: {self.stats['bbw_calc_failed']}")
        print(f"   No signals found: {self.stats['no_signals']}")
        print(f"   Stale/duplicate signals: {self.stats['stale_signals']}")
        print(f"   Fresh signals: {self.stats['fresh_signals']}")
        
        if self.stats['api_errors']:
            print(f"\n❌ API Errors ({len(self.stats['api_errors'])}):")
            for error in self.stats['api_errors'][:5]:  # Show first 5
                print(f"   - {error}")
        
        if self.stats['exchange_responses']:
            print(f"\n📊 Sample Exchange Responses:")
            for response in self.stats['exchange_responses'][:5]:
                print(f"   - {response}")
        
        print(f"\n🚨 Final Results:")
        print(f"   BBW signals detected: {len(detected_signals)}")
        
        if detected_signals:
            print(f"   Signals found:")
            for signal in detected_signals:
                print(f"      - {signal['symbol']}: BBW={signal['bbw_value']:.6f}")
        
        # Send alerts if any
        alerts_sent = 0
        if detected_signals:
            print(f"\n📱 Sending Telegram alert...")
            success = self.telegram.send_consolidated_bbw_alert(detected_signals)
            if success:
                alerts_sent = 1
                print(f"✅ Alert sent successfully")
            else:
                print(f"❌ Alert sending failed")
        
        elapsed = (time.time() - self.start_time) / 60
        print(f"\n⏱️ Total execution time: {elapsed:.2f} minutes")
        print("=" * 80)

if __name__ == "__main__":
    analyzer = DiagnosticBBWAnalyzer()
    analyzer.run_diagnostic_analysis()
