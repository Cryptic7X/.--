#!/usr/bin/env python3
"""
15-Minute BBW Squeeze Analyzer
Fast detection optimized for daily crypto movers
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
        self.market_data = self.load_market_data()
    
    def load_config(self):
        """Load 15m BBW configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        """Load blocked coins (same logic as existing)"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Blocked coins: {len(blocked_coins)}")
            if blocked_coins and len(blocked_coins) <= 10:
                print(f"   {', '.join(sorted(blocked_coins))}")
                
        except FileNotFoundError:
            print("📝 No blocked coins file - analyzing all")
        except Exception as e:
            print(f"⚠️ Blocked coins error: {e}")
            
        return blocked_coins
    
    def load_market_data(self):
        """Load market data with same filtering logic"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        all_coins = data.get('coins', [])
        major_coins = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOT', 'MATIC', 'LINK', 'AVAX', 'UNI']
        
        priority_coins = []
        regular_coins = []
        blocked_count = 0
        
        for coin in all_coins:
            symbol = coin.get('symbol', '').upper()
            
            if symbol in self.blocked_coins:
                blocked_count += 1
                continue
            
            if symbol in major_coins:
                priority_coins.append(coin)
            else:
                regular_coins.append(coin)
        
        priority_coins.sort(key=lambda x: x.get('market_cap', 0), reverse=True)
        filtered_coins = priority_coins + regular_coins
        
        print(f"📊 Market: {len(all_coins)} total, {blocked_count} blocked, {len(filtered_coins)} to analyze")
        print(f"🎯 Priority: {len(priority_coins)} major coins first")
        
        return filtered_coins
    
    def init_exchanges(self):
        """Initialize exchanges (same as existing)"""
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
            print(f"⚠️ BingX failed: {e}")
        
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin failed: {e}")
        
        return exchanges
    
    def fetch_15m_ohlcv(self, symbol):
        """Fetch 15-minute OHLCV data"""
        for exchange_name, exchange in self.exchanges:
            try:
                # Need ~32 hours for 125 candles = 130 candles for safety
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=130)
                
                if len(ohlcv) < 125:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep UTC for freshness
                df['utc_timestamp'] = df.index
                
                # Convert to IST for display
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) >= 125 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception as e:
                continue
        
        return None, None
    
    def analyze_coin_15m_bbw(self, coin_data):
        """Analyze single coin for 15m BBW squeeze"""
        symbol = coin_data.get('symbol', '').upper()
        
        if symbol in self.blocked_coins:
            return None
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_ohlcv(symbol)
            
            if price_df is None or len(price_df) < 125:
                return None
            
            # Calculate BBW signals
            signals_df = detect_exact_bbw_signals(price_df, self.config)
            
            if signals_df.empty:
                return None
            
            # Get latest squeeze signal
            latest_signal = get_latest_squeeze_signal(signals_df)
            
            if not latest_signal:
                return None
            
            # Check freshness
            signal_timestamp_utc = price_df['utc_timestamp'].iloc[-1]
            
            if self.deduplicator.is_signal_fresh_and_new(symbol, signal_timestamp_utc):
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
            
            return None
            
        except Exception as e:
            print(f"❌ {symbol} 15m analysis failed: {str(e)[:100]}")
            return None
    
    def run_15m_analysis(self):
        """Run complete 15m BBW analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🔥 15-MINUTE BBW SQUEEZE ANALYSIS")
        print("=" * 80)
        print(f"🕐 Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⚡ Timeframe: 15-minute (FAST signals)")
        print(f"🎯 Detecting: BBW squeezes for manual direction analysis")
        print(f"🔍 Analyzing: {len(self.market_data)} coins")
        print(f"🚫 Blocked: {len(self.blocked_coins)} coins")
        print(f"⏰ Freshness: 15 minutes")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Cleanup old signals
        self.deduplicator.cleanup_old_signals()
        
        # Collect 15m squeeze signals
        squeeze_signals = []
        batch_size = 20
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_15m_bbw(coin)
                
                if signal_result:
                    squeeze_signals.append(signal_result)
                    symbol = signal_result['symbol']
                    strength = signal_result['squeeze_strength']
                    print(f"🔥 15M SQUEEZE: {symbol} (strength: {strength:.6f})")
                
                total_analyzed += 1
                time.sleep(0.3)  # Rate limiting
        
        # Send alerts
        if squeeze_signals:
            success = send_consolidated_alert(squeeze_signals)
            if success:
                print(f"\n✅ SENT 15M BBW ALERT: {len(squeeze_signals)} squeezes")
        
        # Summary
        print(f"\n" + "=" * 80)
        print("🔥 15-MINUTE BBW ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"📊 Coins analyzed: {total_analyzed}")
        print(f"🔥 Squeeze signals: {len(squeeze_signals)}")
        print(f"📱 Alert sent: {'Yes' if squeeze_signals else 'No'}")
        print(f"⏰ Next analysis: 15 minutes")
        print("=" * 80)

if __name__ == '__main__':
    analyzer = BBW15mAnalyzer()
    analyzer.run_15m_analysis()
