#!/usr/bin/env python3
"""
BBW 2H Analyzer - CORRECTED 75% Range Logic
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

class BBW2HAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator()

    def load_bbw_dataset(self) -> List[Dict]:
        """Load BBW dataset"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Apply BBW filters (≥$100M cap, ≥$50M vol)
            filtered = [
                c for c in coins
                if c.get('market_cap', 0) >= 100_000_000 and c.get('total_volume', 0) >= 50_000_000
            ]
            
            print(f"📊 Loaded {len(filtered)} coins for BBW 2H")
            return filtered
            
        except Exception as e:
            print(f"❌ Failed loading BBW dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """Analyze single coin for BBW squeeze entry"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch 2H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run BBW analysis
            bbw_result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            if not bbw_result.get('squeeze_signal', False):
                return None

            return {
                'symbol': symbol,
                'signal_type': 'BBW_SQUEEZE',
                'bbw_value': bbw_result.get('bbw', 0),
                'contraction_line': bbw_result.get('lowest_contraction', 0),
                'expansion_line': bbw_result.get('highest_expansion', 0),
                'threshold_75': bbw_result.get('threshold_75', 0),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h'
            }

        except Exception as e:
            print(f"⚠️ BBW analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """Process coins in parallel"""
        signals = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_coin = {executor.submit(self.analyze_single_coin, coin): coin for coin in coins}
            processed = 0
            
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        bbw = result['bbw_value']
                        threshold = result['threshold_75']
                        print(f"🔵 BBW SQUEEZE: {result['symbol']} BBW:{bbw:.1f} ≤ {threshold:.1f}")
                    
                    if processed % 20 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)}")
                        
                except Exception as e:
                    continue
        
        return signals

    def run_bbw_analysis(self):
        """Main BBW analysis execution"""
        print("🔵 BBW 2H ANALYSIS STARTING")
        print("=" * 40)
        
        start_time = datetime.utcnow()
        
        # Load dataset
        coins = self.load_bbw_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} BBW coins")
        print("⚡ Timeframe: 2H | Squeeze: 75% range")
        
        # Process coins
        signals = self.process_coins_parallel(coins)
        
        # Send alerts and report results
        if signals:
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("\n" + "=" * 40)
            print("✅ BBW 2H ANALYSIS COMPLETE")
            print(f"🔵 Squeeze Entries: {len(signals)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print("=" * 40)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No BBW squeeze entries found")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = BBW2HAnalyzer(config)
    analyzer.run_bbw_analysis()

if __name__ == '__main__':
    main()
