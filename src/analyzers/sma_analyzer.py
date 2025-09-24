#!/usr/bin/env python3
"""
SMA 2H Analyzer - Fixed to work with existing SMA class
"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.sma import SMAIndicator  # Import the class, not function
from src.alerts.sma_telegram import SMATelegramSender

class SMA2HAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = SMATelegramSender(config)
        
        # Create SMA indicator with config
        sma_config = config.get('sma_config', {})
        self.sma_indicator = SMAIndicator(
            ma_period=sma_config.get('ma_period', 200),
            proximity_threshold=sma_config.get('proximity_threshold', 2.0),
            suppression_hours=sma_config.get('suppression', {}).get('proximity_hours', 12)
        )

    def load_sma_dataset(self) -> List[Dict]:
        """Load SMA dataset (larger dataset)"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'sma_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            print(f"📊 Loaded {len(coins)} coins for SMA (≥$10M cap, ≥$10M vol)")
            return coins
            
        except Exception as e:
            print(f"❌ Failed loading SMA dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """Analyze single coin for SMA signals"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch 2H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run SMA analysis using the existing class method
            sma_result = self.sma_indicator.calculate_sma_signals(ohlcv_data, symbol)
            
            # Check if any signal detected
            if not sma_result.get('signal_detected', False):
                return None
            
            # Return signal data
            return {
                'symbol': symbol,
                'signal_type': sma_result.get('signal_type', 'SMA_SIGNAL'),
                'sma_data': sma_result,
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h'
            }
            
        except Exception as e:
            print(f"⚠️ SMA analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """Process coins in parallel"""
        max_workers = self.config.get('sma_config', {}).get('processing', {}).get('max_workers', 15)
        signals = []
        
        print(f"🔄 Processing {len(coins)} coins with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {executor.submit(self.analyze_single_coin, coin): coin for coin in coins}
            processed = 0
            
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"📈 SMA SIGNAL: {result['symbol']} ({result['signal_type']})")
                    
                    if processed % 50 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return signals

    def run_sma_analysis(self):
        """Main SMA analysis execution"""
        print("📈 SMA 2H ANALYSIS STARTING")
        print("=" * 50)
        
        start_time = datetime.utcnow()
        
        # Load dataset
        coins = self.load_sma_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} SMA coins (≥$10M cap, ≥$10M vol)")
        print("⚡ Timeframe: 2H | 12H Suppression")
        
        # Process coins
        signals = self.process_coins_parallel(coins)
        
        # Send alerts and report results
        if signals:
            success = self.telegram_sender.send_sma_batch_alert(signals, {
                'total_crossovers': sum(1 for s in signals if 'crossover' in s.get('signal_type', '')),
                'total_ranging': sum(1 for s in signals if 'ranging' in s.get('signal_type', ''))
            })
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("\n" + "=" * 50)
            print("✅ SMA 2H ANALYSIS COMPLETE")
            print(f"📈 Signals Found: {len(signals)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Processed: {len(coins)}")
            print("=" * 50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No SMA signals found in this 2H cycle")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Analyzed: {len(coins)}")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = SMA2HAnalyzer(config)
    analyzer.run_sma_analysis()

if __name__ == '__main__':
    main()
