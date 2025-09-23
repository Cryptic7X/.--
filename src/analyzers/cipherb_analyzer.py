#!/usr/bin/env python3
"""
CipherB 2H Analyzer - Parallel Processing with No Suppression
Uses your EXACT CipherB indicator logic
"""

import os
import json
import sys
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.cipherb import detect_exact_cipherb_signals
from src.indicators.bbw import BBWIndicator
from src.indicators.sma import SMAIndicator
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
        Analyze single coin for CipherB signals - WITH DEBUG
        """
        symbol = coin_data['symbol']
        
        try:
            print(f"\n🔍 Analyzing {symbol}...")
            
            # Fetch 2H OHLCV data with fallback
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                print(f"   ❌ No OHLCV data for {symbol}")
                return None
            
            print(f"   ✅ Got {len(ohlcv_data.get('timestamp', []))} candles from {exchange_used}")
            
            # DEBUG: Show data format before CipherB
            if len(ohlcv_data.get('timestamp', [])) > 0:
                print(f"   📊 Data format check:")
                print(f"      Timestamps: {type(ohlcv_data['timestamp'])}, len={len(ohlcv_data['timestamp'])}")
                print(f"      Close prices: {type(ohlcv_data['close'])}, len={len(ohlcv_data['close'])}")
                print(f"      First close: {ohlcv_data['close'][0]} (type: {type(ohlcv_data['close'][0])})")
            
            # Your EXACT CipherB analysis with DEBUG
            print(f"   🎯 Running CipherB analysis...")
            cipherb_config = self.config['cipherb']
            
            # Import debug function
            from src.indicators.cipherb import debug_ohlcv_data, detect_cipherb_signals_2h
            
            # Debug data format
            debug_ohlcv_data(ohlcv_data, symbol)
            
            # Run CipherB
            signals_df = detect_cipherb_signals_2h(ohlcv_data, cipherb_config)
            
            if signals_df.empty:
                print(f"   📭 No signals for {symbol}")
                return None
            
            # Check for signals in latest candle
            latest_signal = signals_df.iloc[-1]
            
            if not (latest_signal['buySignal'] or latest_signal['sellSignal']):
                print(f"   📭 No active signals for {symbol}")
                return None
            
            print(f"   🎯 SIGNAL FOUND for {symbol}!")
            
            # Get additional context for display (BBW and SMA)
            bbw_context = self.get_bbw_context(ohlcv_data)
            sma_context = self.get_sma_context(ohlcv_data)
            
            # Prepare signal data
            signal_data = {
                'symbol': symbol,
                'signal_type': 'BUY' if latest_signal['buySignal'] else 'SELL',
                'wt1': round(latest_signal['wt1'], 1),
                'wt2': round(latest_signal['wt2'], 1),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h',
                
                # Additional context for enhanced alerts
                'bbw_context': bbw_context,
                'sma_context': sma_context
            }
            
            return signal_data
            
        except Exception as e:
            print(f"❌ CipherB analysis failed for {symbol}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def get_bbw_context(self, ohlcv_data: Dict) -> Dict:
        """Get BBW context for display"""
        try:
            bbw_indicator = BBWIndicator(self.config['bbw'])
            return bbw_indicator.get_bbw_context(ohlcv_data)
        except Exception:
            return {'bbw': 0, 'status': 'no_data'}

    def get_sma_context(self, ohlcv_data: Dict) -> Dict:
        """Get SMA context for display"""
        try:
            sma_indicator = SMAIndicator(self.config['sma'])
            return sma_indicator.get_sma_context(ohlcv_data)
        except Exception:
            return {'sma50': 0, 'sma200': 0, 'trend': 'no_data'}

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """
        Process multiple coins in parallel
        """
        max_workers = self.config['cipherb'].get('max_workers', 10)
        signals = []
        
        print(f"🔄 Processing {len(coins)} coins with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all coins for processing
            future_to_coin = {
                executor.submit(self.analyze_single_coin, coin): coin 
                for coin in coins
            }
            
            # Collect results as they complete
            processed = 0
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ {result['signal_type']} signal: {result['symbol']}")
                    
                    # Progress update every 50 coins
                    if processed % 50 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return signals

    def run_cipherb_analysis(self):
        """
        Main CipherB 2H analysis function
        """
        print("🎯 CIPHERB 2H ANALYSIS STARTING")
        print("="*50)
        
        start_time = datetime.utcnow()
        
        # Load CipherB dataset
        coins = self.load_cipherb_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} CipherB coins (≥$100M cap, ≥$10M vol)")
        print(f"⚡ Timeframe: 2H | No Suppression: Every signal captured")
        
        # Process coins in parallel
        signals = self.process_coins_parallel(coins)
        
        # Send alerts if any signals found
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
