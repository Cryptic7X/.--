#!/usr/bin/env python3
"""
BBW 30-Minute Squeeze Analyzer
Automated detection of Bollinger BandWidth squeeze signals
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from indicators.bbw_pinescript_exact import BBWIndicator
from alerts.telegram_coinglass import TelegramCoinGlassAlert
from alerts.deduplication_30m import BBWDeduplicator

def get_ist_time():
    """Convert UTC to IST for proper timezone display"""
    utc_now = datetime.utcnow()
    ist_time = utc_now + timedelta(hours=5, minutes=30)
    return ist_time

class BBW30mAnalyzer:
    def __init__(self):
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
            if blocked_coins:
                print(f"   Blocked: {', '.join(sorted(list(blocked_coins)[:10]))}")
                if len(blocked_coins) > 10:
                    print(f"   ... and {len(blocked_coins) - 10} more")
                    
        except FileNotFoundError:
            print("⚠️  No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"❌ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data(self):
        """Load market data and filter out blocked coins"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_12h.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found. Run datafetcher first.")
            return []
            
        with open(cache_file) as f:
            data = json.load(f)
            
        coins = data.get('coins', [])
        
        # Filter out blocked coins
        filtered_coins = []
        for coin in coins:
            symbol = coin.get('symbol', '').upper()
            if symbol not in self.blocked_coins:
                filtered_coins.append(coin)
        
        print(f"📊 Market data loaded: {len(coins)} total, {len(filtered_coins)} after filtering")
        return filtered_coins[:self.config['market_filter']['max_coins_analyzed']]
    
    def init_exchanges(self):
        """Initialize exchange connections with fallback order"""
        exchanges = []
        
        # BingX (Primary - your personal API)
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': self.config['apis']['exchanges']['bingx']['rate_limit'],
                'enableRateLimit': True,
                'timeout': self.config['apis']['exchanges']['bingx']['timeout'],
            })
            exchanges.append(('BingX', bingx))
            print("✅ BingX initialized (Primary)")
        except Exception as e:
            print(f"❌ BingX initialization failed: {e}")
        
        # Fallback exchanges (India-friendly)
        fallback_exchanges = [
            ('KuCoin', ccxt.kucoin, 'kucoin'),
            ('OKX', ccxt.okx, 'okx'),
            ('Bybit', ccxt.bybit, 'bybit')
        ]
        
        for name, exchange_class, config_key in fallback_exchanges:
            try:
                exchange = exchange_class({
                    'rateLimit': self.config['apis']['exchanges'][config_key]['rate_limit'],
                    'enableRateLimit': True,
                    'timeout': self.config['apis']['exchanges'][config_key]['timeout'],
                })
                exchanges.append((name, exchange))
                print(f"✅ {name} initialized (Fallback)")
            except Exception as e:
                print(f"⚠️  {name} initialization failed: {e}")
        
        return exchanges
    
    def fetch_ohlcv_data(self, symbol, timeframe='30m', required_candles=150):
        """Fetch OHLCV data with exchange fallback"""
        for exchange_name, exchange in self.exchanges:
            try:
                # Fetch candles
                ohlcv = exchange.fetch_ohlcv(f"{symbol}USDT", timeframe, limit=required_candles)
                
                if len(ohlcv) < 125:  # Need minimum for BBW calculation
                    continue
                    
                # Convert to DataFrame
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep UTC timestamps for freshness checking
                df['utc_timestamp'] = df.index
                
                # Convert to IST for display
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) >= 125 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception as e:
                continue
                
        return None, None
    
    def analyze_coin_bbw_signals(self, coin_data):
        """Analyze coin for BBW squeeze signals"""
        symbol = coin_data.get('symbol', '').upper()
        
        # Skip blocked coins
        if symbol in self.blocked_coins:
            return None
            
        try:
            # Fetch OHLCV data
            df, exchange_used = self.fetch_ohlcv_data(symbol, '30m')
            
            if df is None:
                return None
            
            # Initialize BBW indicator
            bbw_indicator = BBWIndicator(
                length=self.config['bbw']['length'],
                multiplier=self.config['bbw']['multiplier'],
                contraction_length=self.config['bbw']['contraction_length']
            )
            
            # Calculate BBW signals
            signals_df = bbw_indicator.calculate_bbw_signals(df)
            
            if signals_df.empty:
                return None
            
            # Get latest squeeze signal
            latest_signal = bbw_indicator.get_latest_squeeze_signal(
                signals_df,
                tolerance=self.config['bbw']['squeeze_tolerance'],
                separation_threshold=self.config['bbw']['separation_threshold']
            )
            
            if not latest_signal:
                return None
            
            # Check signal freshness
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
            
        except Exception as e:
            print(f"❌ {symbol} analysis failed: {str(e)[:100]}")
            return None
    
    def run_bbw_analysis(self):
        """Run complete BBW 30-minute analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🎯 BBW 30-MINUTE SQUEEZE ANALYSIS")
        print("=" * 80)
        print(f"📅 Analysis Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"🎯 Detecting BBW squeeze signals (manual direction analysis)")
        print(f"📊 Coins to analyze: {len(self.market_data)}")
        print(f"📛 Blocked coins: {len(self.blocked_coins)}")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # Collect signals
        detected_signals = []
        batch_size = self.config['system']['batch_size']
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_bbw_signals(coin)
                if signal_result:
                    detected_signals.append(signal_result)
                    print(f"🚨 BBW SQUEEZE: {signal_result['symbol']}")
                
                total_analyzed += 1
                time.sleep(self.config['system']['rate_limit_delay'])  # Rate limiting
        
        # Send alerts
        alerts_sent = 0
        if detected_signals:
            success = self.telegram.send_consolidated_bbw_alert(detected_signals)
            if success:
                alerts_sent = 1
                print(f"📱 SENT BBW ALERT: {len(detected_signals)} signals")
        
        # Summary
        print("=" * 80)
        print("🎯 BBW ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"📊 Coins analyzed: {total_analyzed}")
        print(f"🚨 BBW squeezes detected: {len(detected_signals)}")
        print(f"📱 Alerts sent: {alerts_sent}")
        print("=" * 80)

if __name__ == "__main__":
    analyzer = BBW30mAnalyzer()
    analyzer.run_bbw_analysis()
