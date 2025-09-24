#!/usr/bin/env python3
"""
BBW 2H Analyzer - OPTIMIZED FOR SPEED
Should complete analysis in 15-30 seconds maximum
"""
import os
import json
import sys
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.bbw import BBWIndicator
from src.alerts.bbw_telegram import BBWTelegramSender

class FastBBW2HAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator()

    def load_bbw_dataset(self) -> List[Dict]:
        """Fast dataset loading"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Apply filters and limit to top coins for speed
            filtered = [
                c for c in coins
                if c.get('market_cap', 0) >= 100_000_000 and c.get('total_volume', 0) >= 50_000_000
            ]
            
            # Sort by market cap and limit to top 100 coins for speed
            filtered = sorted(filtered, key=lambda x: x.get('market_cap', 0), reverse=True)[:100]
            
            print(f"📊 Loaded top {len(filtered)} coins for BBW 2H (speed optimized)")
            return filtered
            
        except Exception as e:
            print(f"❌ Dataset load failed: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """Fast single coin analysis"""
        symbol = coin_data['symbol']
        
        try:
            # Fast OHLCV fetch (reduced limit)
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=100
            )
            
            if not ohlcv_data or len(ohlcv_data.get('close', [])) < 50:
                return None
            
            # Fast BBW analysis
            bbw_result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            if not bbw_result.get('squeeze_signal', False):
                return None

            return {
                'symbol': symbol,
                'bbw_value': bbw_result.get('bbw', 0),
                'contraction_line': bbw_result.get('lowest_contraction', 0),
                'expansion_line': bbw_result.get('highest_expansion', 0),
                'threshold_75': bbw_result.get('threshold_75', 0),
                'coin_data': coin_data,
                'exchange_used': exchange_used
            }

        except Exception:
            return None

    def run_bbw_analysis(self):
        """OPTIMIZED BBW analysis execution"""
        print("🔵 BBW 2H ANALYSIS - SPEED OPTIMIZED")
        print("=" * 40)
        
        start_time = datetime.utcnow()
        
        # Load dataset (limited for speed)
        coins = self.load_bbw_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing top {len(coins)} coins")
        print("⚡ Speed mode: 5s timeouts, 100 candles, 12 workers")
        
        # Fast parallel processing
        signals = []
        completed = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            # Submit all jobs
            future_to_coin = {
                executor.submit(self.analyze_single_coin, coin): coin 
                for coin in coins
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_coin, timeout=60):
                coin = future_to_coin[future]
                completed += 1
                
                try:
                    result = future.result(timeout=2)  # Fast timeout per result
                    if result:
                        signals.append(result)
                        print(f"🔵 SQUEEZE: {result['symbol']} ({result['bbw_value']:.1f})")
                except:
                    pass  # Skip errors for speed
                
                # Progress every 20 coins
                if completed % 20 == 0:
                    print(f"📈 {completed}/{len(coins)} processed")
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Save cache updates in batch
        self.bbw_indicator.save_cache_updates({})
        
        # Send results
        if signals:
            print(f"\n🔵 Found {len(signals)} squeeze signals")
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            print(f"📱 Alert sent: {'Yes' if success else 'Failed'}")
        else:
            print(f"\n📭 No squeeze signals found")
        
        print(f"⚡ SPEED: {processing_time:.1f}s total")
        print("=" * 40)

def main():
    try:
        import yaml
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        analyzer = FastBBW2HAnalyzer(config)
        analyzer.run_bbw_analysis()
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")

if __name__ == '__main__':
    main()
