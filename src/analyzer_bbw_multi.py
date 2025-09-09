#!/usr/bin/env python3
"""
BBW Multi-Timeframe Analyzer

Analyzes 30-minute and 1-hour candles for BBW squeeze signals
- Prioritizes aligned signals (both timeframes squeezed)
- Independent timeframe analysis with separate freshness windows
- Maintains blocked coins filtering
- Exact Pine Script BBW replication
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
from alerts.deduplication_bbw import BBWSignalDeduplicator
from indicators.bbw_exact import detect_exact_bbw_signals, get_latest_squeeze_signal

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class BBWMultiTimeframeAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.deduplicator = BBWSignalDeduplicator()
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data()
    
    def load_config(self):
        """Load BBW configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        """Load blocked coins from blocked_coins.txt"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins")
            if blocked_coins:
                print(f"   Blocked: {', '.join(sorted(list(blocked_coins)[:10]))}")
                if len(blocked_coins) > 10:
                    print(f"   ... and {len(blocked_coins) - 10} more")
                    
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins
    
    def load_market_data(self):
        """Load market data and filter out blocked coins"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        all_coins = data.get('coins', [])
        
        # Priority coins for faster processing
        major_coins_list = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOT', 'MATIC', 'LINK', 'AVAX', 'UNI']
        priority_coins = []
        regular_coins = []
        blocked_count = 0
        
        for coin in all_coins:
            symbol = coin.get('symbol', '').upper()
            
            if symbol in self.blocked_coins:
                blocked_count += 1
                continue
            
            if symbol in major_coins_list:
                priority_coins.append(coin)
            else:
                regular_coins.append(coin)
        
        # Sort priority coins by market cap
        priority_coins.sort(key=lambda x: x.get('market_cap', 0), reverse=True)
        filtered_coins = priority_coins + regular_coins
        
        print(f"📊 Market data: {len(all_coins)} total, {blocked_count} blocked, {len(filtered_coins)} to analyze")
        print(f"🎯 Priority coins: {len(priority_coins)} major coins processed first")
        
        return filtered_coins
    
    def init_exchanges(self):
        """Initialize exchange connections"""
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
    
    def fetch_ohlcv_data(self, symbol, timeframe, required_candles=130):
        """
        Fetch OHLCV data for specific timeframe
        Ensures same exchange for both timeframes per coin
        """
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", timeframe, limit=required_candles)
                
                if len(ohlcv) < 125:  # Need minimum for BBW calculation
                    continue
                
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
        """
        Analyze coin for BBW squeeze signals on both timeframes
        Returns signal data or None
        """
        symbol = coin_data.get('symbol', '').upper()
        
        # Skip blocked coins
        if symbol in self.blocked_coins:
            return None
        
        try:
            # Fetch data for both timeframes from same exchange
            data_30m, exchange_30m = self.fetch_ohlcv_data(symbol, '30m')
            data_1h, exchange_1h = self.fetch_ohlcv_data(symbol, '1h')
            
            # Require same exchange for consistency
            if data_30m is None or data_1h is None or exchange_30m != exchange_1h:
                return None
            
            # Calculate BBW signals for both timeframes
            signals_30m = detect_exact_bbw_signals(data_30m, self.config)
            signals_1h = detect_exact_bbw_signals(data_1h, self.config)
            
            if signals_30m.empty or signals_1h.empty:
                return None
            
            # Get latest signals
            latest_30m = get_latest_squeeze_signal(signals_30m)
            latest_1h = get_latest_squeeze_signal(signals_1h)
            
            # Check freshness and deduplication
            signal_30m_fresh = False
            signal_1h_fresh = False
            
            if latest_30m:
                signal_30m_timestamp = data_30m['utc_timestamp'].iloc[-1]
                signal_30m_fresh = self.deduplicator.is_signal_fresh_and_new(
                    symbol, '30m', signal_30m_timestamp
                )
            
            if latest_1h:
                signal_1h_timestamp = data_1h['utc_timestamp'].iloc[-1]  
                signal_1h_fresh = self.deduplicator.is_signal_fresh_and_new(
                    symbol, '1h', signal_1h_timestamp
                )
            
            # Determine signal priority and type
            if signal_30m_fresh and signal_1h_fresh:
                # Both timeframes aligned - highest priority
                if self.deduplicator.is_aligned_signal_new(symbol, signal_30m_timestamp, signal_1h_timestamp):
                    return {
                        'symbol': symbol,
                        'signal_type': 'aligned',
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_30m,
                        'bbw_30m_value': latest_30m['bbw_value'],
                        'lowest_contraction_30m': latest_30m['lowest_contraction'],
                        'bbw_1h_value': latest_1h['bbw_value'],
                        'lowest_contraction_1h': latest_1h['lowest_contraction'],
                        'timestamp_30m': signals_30m.index[-1],
                        'timestamp_1h': signals_1h.index[-1],
                        'coin_data': coin_data
                    }
            
            elif signal_30m_fresh:
                # Only 30m signal
                return {
                    'symbol': symbol,
                    'signal_type': '30m',
                    'timeframe': '30m',
                    'price': coin_data.get('current_price', 0),
                    'change_24h': coin_data.get('price_change_percentage_24h', 0),
                    'market_cap': coin_data.get('market_cap', 0),
                    'exchange': exchange_30m,
                    'bbw_value': latest_30m['bbw_value'],
                    'lowest_contraction': latest_30m['lowest_contraction'],
                    'timestamp': signals_30m.index[-1],
                    'coin_data': coin_data
                }
            
            elif signal_1h_fresh:
                # Only 1h signal  
                return {
                    'symbol': symbol,
                    'signal_type': '1h',
                    'timeframe': '1h',
                    'price': coin_data.get('current_price', 0),
                    'change_24h': coin_data.get('price_change_percentage_24h', 0),
                    'market_cap': coin_data.get('market_cap', 0),
                    'exchange': exchange_1h,
                    'bbw_value': latest_1h['bbw_value'],
                    'lowest_contraction': latest_1h['lowest_contraction'],
                    'timestamp': signals_1h.index[-1],
                    'coin_data': coin_data
                }
            
            return None
            
        except Exception as e:
            print(f"❌ {symbol} analysis failed: {str(e)[:100]}")
            return None
    
    def run_bbw_analysis(self):
        """Run complete BBW multi-timeframe analysis"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🎯 BBW MULTI-TIMEFRAME SQUEEZE ANALYSIS")
        print("=" * 80)
        print(f"🕐 Analysis Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏰ Timeframes: 30m + 1h")
        print(f"🎯 Detecting: BBW squeeze signals (manual direction analysis)")
        print(f"🔍 Coins to analyze: {len(self.market_data)}")
        print(f"🚫 Blocked coins: {len(self.blocked_coins)}")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Clean up old signals
        self.deduplicator.cleanup_old_signals()
        
        # Collect signals by priority
        aligned_signals = []
        individual_30m_signals = []
        individual_1h_signals = []
        
        batch_size = 20
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_bbw_signals(coin)
                
                if signal_result:
                    symbol = signal_result['symbol']
                    signal_type = signal_result['signal_type']
                    
                    if signal_type == 'aligned':
                        aligned_signals.append(signal_result)
                        print(f"🚨 ALIGNED: {symbol} (both 30m + 1h squeezed)")
                    elif signal_type == '30m':
                        individual_30m_signals.append(signal_result)
                        print(f"🔥 30M: {symbol} (squeeze detected)")
                    elif signal_type == '1h':
                        individual_1h_signals.append(signal_result)
                        print(f"🔥 1H: {symbol} (squeeze detected)")
                
                total_analyzed += 1
                time.sleep(0.3)  # Rate limiting
        
        # Send alerts based on priority
        alerts_sent = 0
        
        # Priority 1: Aligned signals (both timeframes)
        if aligned_signals:
            success = send_consolidated_alert(aligned_signals, 'aligned')
            if success:
                alerts_sent += 1
                print(f"✅ SENT ALIGNED BBW ALERT: {len(aligned_signals)} coins")
        
        # Priority 2: Individual 30m signals (if no aligned signals sent)
        if individual_30m_signals and not aligned_signals:
            success = send_consolidated_alert(individual_30m_signals, 'individual')
            if success:
                alerts_sent += 1
                print(f"✅ SENT 30M BBW ALERT: {len(individual_30m_signals)} coins")
        
        # Priority 3: Individual 1h signals (if no other signals sent)
        if individual_1h_signals and not aligned_signals and not individual_30m_signals:
            success = send_consolidated_alert(individual_1h_signals, 'individual') 
            if success:
                alerts_sent += 1
                print(f"✅ SENT 1H BBW ALERT: {len(individual_1h_signals)} coins")
        
        # Summary
        total_signals = len(aligned_signals) + len(individual_30m_signals) + len(individual_1h_signals)
        
        print(f"\n" + "=" * 80)
        print("🎯 BBW MULTI-TIMEFRAME ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"📊 Coins analyzed: {total_analyzed}")
        print(f"🚨 Aligned signals: {len(aligned_signals)}")
        print(f"🔥 30m signals: {len(individual_30m_signals)}")
        print(f"🔥 1h signals: {len(individual_1h_signals)}")
        print(f"📱 Total alerts sent: {alerts_sent}")
        print(f"⏰ Next analysis: 30 minutes")
        print("=" * 80)

if __name__ == '__main__':
    analyzer = BBWMultiTimeframeAnalyzer()
    analyzer.run_bbw_analysis()
