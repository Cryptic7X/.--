#!/usr/bin/env python3
"""
SMA 2H Analyzer - Crossovers + Proximity with 12H Suppression
NO RANGING ALERTS - FIXED CONFIG HANDLING
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
from src.indicators.sma import SMAIndicator  # Direct class import instead of function
from src.alerts.sma_telegram import SMATelegramSender

class SMA2HAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = SMATelegramSender(config)
        
        # Initialize SMA indicator with config values
        sma_config = config.get('sma', {})
        short_length = sma_config.get('short_length', 50)
        long_length = sma_config.get('long_length', 200)
        self.sma_indicator = SMAIndicator(short_length=short_length, long_length=long_length)

    def load_sma_dataset(self) -> List[Dict]:
        """Load SMA coin dataset (larger dataset)"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'sma_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            print(f"📊 Loaded {len(coins)} SMA coins from cache (≥$10M cap/vol)")
            return coins
            
        except FileNotFoundError:
            print("❌ SMA dataset not found. Run daily data fetcher first.")
            return []
        except Exception as e:
            print(f"❌ Error loading SMA dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """
        Analyze single coin for SMA crossover and proximity signals
        NO RANGING LOGIC
        """
        symbol = coin_data['symbol']
        
        try:
            # Fetch 2H OHLCV data with fallback
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=250  # Need at least 200 for SMA200
            )
            
            if not ohlcv_data:
                return None

            # Get current price
            current_price = ohlcv_data['close'][-1] if ohlcv_data['close'] else 0

            # Use SMA indicator directly (NO RANGING)
            result = self.sma_indicator.calculate_sma_signals(ohlcv_data, symbol, current_price)
            
            if result.get('error'):
                return None

            crossover_alerts = result.get('crossover_alerts', [])
            proximity_alerts = result.get('proximity_alerts', [])

            all_alerts = crossover_alerts + proximity_alerts
            if not all_alerts:
                return None

            # Prepare combined signal data
            signal_data = {
                'symbol': symbol,
                'crossover_alerts': crossover_alerts,
                'proximity_alerts': proximity_alerts,
                'total_alerts': len(all_alerts),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h'
            }
            
            return signal_data

        except Exception as e:
            print(f"⚠️ SMA analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """
        Process multiple coins in parallel (larger dataset)
        """
        # Get max_workers from config with safe fallback
        sma_config = self.config.get('sma', {})
        max_workers = sma_config.get('max_workers', 15)  # Fallback to 15
        signals = []
        
        print(f"🔄 Processing {len(coins)} coins with {max_workers} workers...")
        
        # Process in batches to manage large dataset
        performance_config = self.config.get('performance', {})
        batch_size = performance_config.get('batch_size', 100)
        
        for i in range(0, len(coins), batch_size):
            batch = coins[i:i + batch_size]
            print(f"📦 Processing batch {i//batch_size + 1}: {len(batch)} coins")
            
            batch_signals = self.process_batch_parallel(batch, max_workers)
            signals.extend(batch_signals)
            
            # Small delay between batches
            if i + batch_size < len(coins):
                import time
                time.sleep(1)
        
        return signals

    def process_batch_parallel(self, batch_coins: List[Dict], max_workers: int) -> List[Dict]:
        """Process a batch of coins in parallel"""
        batch_signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch for processing
            future_to_coin = {
                executor.submit(self.analyze_single_coin, coin): coin
                for coin in batch_coins
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                try:
                    result = future.result(timeout=30)
                    if result:
                        batch_signals.append(result)
                        
                        # Log different types of SMA signals
                        for alert in result['crossover_alerts']:
                            if alert['type'] == 'golden_cross':
                                print(f"🟡 GOLDEN CROSS: {result['symbol']}")
                            elif alert['type'] == 'death_cross':
                                print(f"🔴 DEATH CROSS: {result['symbol']}")
                        
                        for alert in result['proximity_alerts']:
                            print(f"📍 {alert['type'].upper()}: {result['symbol']} ({alert['level']})")
                            
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return batch_signals

    def run_sma_analysis(self):
        """
        Main SMA 2H analysis function
        NO RANGING ALERTS
        """
        print("🟡 SMA 2H ANALYSIS STARTING")
        print("="*50)
        
        start_time = datetime.utcnow()
        
        # Load SMA dataset
        coins = self.load_sma_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} SMA coins (≥$10M cap, ≥$10M vol)")
        print(f"⚡ Timeframe: 2H | Crossovers: No suppression | Proximity: 12H suppression")
        print("❌ Ranging alerts: DISABLED")
        
        # Process coins in parallel batches
        signals = self.process_coins_parallel(coins)
        
        # Calculate summary statistics (NO RANGING)
        total_crossovers = sum(len(s['crossover_alerts']) for s in signals)
        total_proximity = sum(len(s['proximity_alerts']) for s in signals)
        
        # Send alerts if any signals found
        if signals:
            success = self.telegram_sender.send_sma_batch_alert(signals, {
                'total_crossovers': total_crossovers,
                'total_proximity': total_proximity
            })
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("\n" + "="*50)
            print("✅ SMA 2H ANALYSIS COMPLETE")
            print(f"🟡 Crossovers: {total_crossovers}")
            print(f"📍 Proximity Alerts: {total_proximity}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Processed: {len(coins)}")
            print("="*50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No SMA signals found in this 2H cycle")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Analyzed: {len(coins)}")

def main():
    import yaml
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Run SMA analysis
    analyzer = SMA2HAnalyzer(config)
    analyzer.run_sma_analysis()

if __name__ == '__main__':
    main()
