#!/usr/bin/env python3
"""
CipherB 2H Analyzer - 100% Pine Script Match
Fully independent system with your exact indicator logic
"""

import os
import json
import sys
import pandas as pd
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.cipherb import detect_exact_cipherb_signals
from src.alerts.cipherb_telegram import CipherBTelegramSender

class CipherB2HAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = CipherBTelegramSender(config)
        
    def load_cipherb_dataset(self) -> List[Dict]:
        """Load CipherB coin dataset"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
                print(f"📊 Loaded {len(coins)} CipherB coins from cache")
                return coins
        except FileNotFoundError:
            print("❌ CipherB dataset not found. Run daily data fetcher first.")
            return []
        except Exception as e:
            print(f"❌ Error loading CipherB dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """
        Analyze single coin for CipherB signals - 2H TIMEFRAME EXACT MATCH
        """
        symbol = coin_data['symbol']
        
        try:
            # Fetch 2H OHLCV data (matching your TradingView timeframe)
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200  # Enough data for indicator calculations
            )
            
            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 50:
                return None
            
            # Create DataFrame - simple and clean
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'], 
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            
            # Convert timestamp and set as index
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Convert to float and handle NaN
            df = df.astype(float)
            df = df.ffill().bfill()
            
            if len(df) < 50:
                return None
            
            # Use YOUR EXACT CipherB function with Pine Script logic
            cipherb_config = {
                'wt_channel_len': 9,      # Your exact value
                'wt_average_len': 12,     # Your exact value
                'wt_ma_len': 3,           # Your exact value
                'oversold_threshold': -60, # Your exact value
                'overbought_threshold': 60 # Your exact value
            }
            
            signals_df = detect_exact_cipherb_signals(df, cipherb_config)
            
            if signals_df.empty:
                return None
            
            # Check for signals in latest candle (most recent 2H period)
            latest_signal = signals_df.iloc[-1]
            
            if not (latest_signal['buySignal'] or latest_signal['sellSignal']):
                return None
            
            # Prepare signal data - CLEAN AND INDEPENDENT
            signal_data = {
                'symbol': symbol,
                'signal_type': 'BUY' if latest_signal['buySignal'] else 'SELL',
                'wt1': round(latest_signal['wt1'], 1),
                'wt2': round(latest_signal['wt2'], 1),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h'
            }
            
            return signal_data
            
        except Exception as e:
            print(f"⚠️ CipherB analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """Process coins in parallel"""
        max_workers = self.config['cipherb'].get('max_workers', 10)
        signals = []
        
        print(f"🔄 Processing {len(coins)} coins with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {
                executor.submit(self.analyze_single_coin, coin): coin 
                for coin in coins
            }
            
            processed = 0
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ {result['signal_type']} signal: {result['symbol']} (WT1: {result['wt1']}, WT2: {result['wt2']})")
                    
                    if processed % 50 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return signals

    def run_cipherb_analysis(self):
        """Main CipherB 2H analysis function"""
        print("🎯 CIPHERB 2H ANALYSIS STARTING")
        print("="*50)
        
        start_time = datetime.utcnow()
        
        # Load CipherB dataset
        coins = self.load_cipherb_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} CipherB coins (≥$100M cap, ≥$10M vol)")
        print(f"⚡ Timeframe: 2H | Exact Pine Script Match | No Suppression")
        
        # Process coins in parallel
        signals = self.process_coins_parallel(coins)
        
        # Send alerts if signals found
        if signals:
            success = self.telegram_sender.send_cipherb_batch_alert(signals)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("\n" + "="*50)
            print("✅ CIPHERB 2H ANALYSIS COMPLETE")
            print(f"🎯 Signals Found: {len(signals)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Processed: {len(coins)}")
            print("="*50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No CipherB signals found in this 2H cycle")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Analyzed: {len(coins)}")

def main():
    import yaml
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Run CipherB analysis
    analyzer = CipherB2HAnalyzer(config)
    analyzer.run_cipherb_analysis()

if __name__ == '__main__':
    main()
