#!/usr/bin/env python3
"""
BBW 30M Analyzer - 100% Range Logic
- Only analyzes coins with Market Cap ≥ $100M and 24h Volume ≥ $50M  
- Processes 30-minute candles only
- Alerts when BBW first enters 100% range (from contraction to expansion)
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

MIN_MARKET_CAP = 100_000_000  # $100M
MIN_VOLUME_24H = 50_000_000   # $50M

class BBW30MAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator()

    def load_bbw_dataset(self) -> List[Dict]:
        """Load BBW dataset with same market cap/volume filters"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
            
            # Apply same BBW filters as before
            filtered = [
                c for c in coins
                if c.get('market_cap', 0) >= MIN_MARKET_CAP and c.get('total_volume', 0) >= MIN_VOLUME_24H
            ]
            
            print(f"📊 Loaded {len(filtered)} coins for BBW 30M (Market Cap ≥ $100M and 24h Vol ≥ $50M)")
            return filtered
            
        except Exception as e:
            print(f"❌ Failed loading BBW dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """Analyze single coin for BBW 100% range entry"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch 30M OHLCV data (changed from 15m)
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '30m', limit=200
            )
            
            if not ohlcv_data:
                return None
            
            # Run BBW analysis with 100% range logic
            bbw_result = self.bbw_indicator.calculate_bbw_signals(ohlcv_data, symbol)
            
            if not bbw_result.get('squeeze_signal', False):
                return None

            return {
                'symbol': symbol,
                'signal_type': 'BBW_100_RANGE',
                'bbw_value': bbw_result.get('bbw', 0),
                'contraction_line': bbw_result.get('lowest_contraction', 0),
                'expansion_line': bbw_result.get('highest_expansion', 0),
                'range_percentage': bbw_result.get('range_percentage', 0),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '30m'
            }

        except Exception as e:
            print(f"⚠️ BBW analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """Process coins in parallel"""
        max_workers = self.config.get('bbw_config', {}).get('processing', {}).get('max_workers', 10)
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
                        print(f"🔵 BBW 100% RANGE: {result['symbol']} (BBW: {result['bbw_value']:.2f})")
                    
                    if processed % 25 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return signals

    def run_bbw_analysis(self):
        """Main BBW analysis execution"""
        print("🔵 BBW 30M ANALYSIS STARTING")
        print("=" * 50)
        
        start_time = datetime.utcnow()
        
        # Load dataset
        coins = self.load_bbw_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} BBW coins (≥$100M cap, ≥$50M vol)")
        print("⚡ Timeframe: 30M | 100% Range Entry Detection")
        
        # Process coins
        signals = self.process_coins_parallel(coins)
        
        # Send alerts and report results
        if signals:
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("\n" + "=" * 50)
            print("✅ BBW 30M ANALYSIS COMPLETE")
            print(f"🔵 100% Range Entries: {len(signals)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Processed: {len(coins)}")
            print("=" * 50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No BBW 100% range entries found in this 30M cycle")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Analyzed: {len(coins)}")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = BBW30MAnalyzer(config)
    analyzer.run_bbw_analysis()

if __name__ == '__main__':
    main()
