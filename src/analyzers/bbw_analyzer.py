#!/usr/bin/env python3
"""
BBW 15M Analyzer
- ONLY runs on coins with Market Cap ≥ $100M and 24h Volume ≥ $50M
- 15-minute timeframe analysis (no more 30-min)
"""

import os
import json
import sys
import pandas as pd
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.exchanges.simple_exchange import SimpleExchangeManager
from src.indicators.bbw import BBWIndicator, detect_bbw_squeeze_signals
from src.alerts.bbw_telegram import BBWTelegramSender

# --- SET YOUR FILTERS HERE ---
MIN_MARKET_CAP = 100_000_000
MIN_VOLUME_24H = 50_000_000
# -----------------------------

class BBW15MAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator(config['bbw'])
        
    def load_bbw_dataset(self) -> List[Dict]:
        """Load and filter BBW coin dataset at runtime by cap and volume"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                coins = data.get('coins', [])
                # -- Runtime Filtering --
                filtered = [
                    c for c in coins
                    if c.get('market_cap', 0) >= MIN_MARKET_CAP
                    and c.get('total_volume', 0) >= MIN_VOLUME_24H
                ]
                print(f"📊 Loaded {len(filtered)}/{len(coins)} BBW coins (≥$100M cap, ≥$50M vol)")
                return filtered
        except FileNotFoundError:
            print("❌ BBW dataset not found. Run daily data fetcher first.")
            return []
        except Exception as e:
            print(f"❌ Error loading BBW dataset: {e}")
            return []

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        symbol = coin_data['symbol']
        try:
            # Fetch 15m OHLCV data with fallback
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '15m', limit=200  # Enough data for squeeze detection
            )
            if not ohlcv_data:
                return None

            # Calculate BBW and detect squeeze
            squeeze_detected, bbw_result = detect_bbw_squeeze_signals(ohlcv_data, self.config['bbw'])
            if not squeeze_detected:
                return None

            # Deduplication/first touch logic
            first_touch_detected, alert_data = self.bbw_indicator.detect_first_touch_contraction(
                symbol, bbw_result['bbw_data']
            )
            if not first_touch_detected:
                return None

            # Prepare signal data
            signal_data = {
                'symbol': symbol,
                'signal_type': 'BBW_SQUEEZE',
                'bbw_value': alert_data['bbw_value'],
                'contraction_line': alert_data['contraction_line'],
                'consecutive_confirmed': alert_data['consecutive_confirmed'],
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '15m'
            }
            return signal_data

        except Exception as e:
            print(f"⚠️ BBW analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        max_workers = self.config['bbw'].get('max_workers', 10)
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
                        print(f"🔵 BBW SQUEEZE: {result['symbol']} (BBW: {result['bbw_value']})")
                    if processed % 25 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        return signals

    def run_bbw_analysis(self):
        print("🔵 BBW 15M ANALYSIS STARTING")
        print("="*50)
        start_time = datetime.utcnow()
        coins = self.load_bbw_dataset()
        if not coins:
            return
        print(f"📊 Analyzing {len(coins)} BBW coins (≥$100M cap, ≥$50M vol)")
        print(f"⚡ Timeframe: 15M | First-Touch Only")
        signals = self.process_coins_parallel(coins)
        if signals:
            success = self.telegram_sender.send_bbw_batch_alert(signals)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print("\n" + "="*50)
            print("✅ BBW 15M ANALYSIS COMPLETE")
            print(f"🔵 Squeezes Found: {len(signals)}")
            print(f"📱 Alert Sent: {'Yes' if success else 'Failed'}")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Processed: {len(coins)}")
            print("="*50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"\n📭 No BBW first-touch squeezes found in this 15M cycle")
            print(f"⏱️ Processing Time: {processing_time:.1f}s")
            print(f"📊 Coins Analyzed: {len(coins)}")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    analyzer = BBW15MAnalyzer(config)
    analyzer.run_bbw_analysis()

if __name__ == '__main__':
    main()
