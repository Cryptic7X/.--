#!/usr/bin/env python3
"""
CipherB 2H Analyzer — STRICT Pine Script Match
Emits alerts ONLY when your Pine Script buy/sell plotshape signal would plot.
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
        with open(cache_file, 'r') as f:
            data = json.load(f)
            coins = data.get('coins', [])
            print(f"📊 Loaded {len(coins)} CipherB coins from cache")
            return coins

    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        symbol = coin_data['symbol']
        
        try:
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )

            # Not enough candles, skip
            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 25:
                return None

            # DataFrame for your indicator
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df.astype(float).ffill().bfill()
            if len(df) < 25:
                return None

            # Run your EXACT Pine Script logic
            signals_df = detect_exact_cipherb_signals(df, {
                'wt_channel_len': 9, 
                'wt_average_len': 12,
                'wt_ma_len': 3,
                'oversold_threshold': -60,
                'overbought_threshold': 60
            })
            if signals_df.empty:
                return None

            # STRICT: Only fire if the LAST COMPLETED 2H candle triggers plotshape (the buy/sell signal)
            # TradingView plots shapes on the *previous* candle, so check the next-to-last candle.
            penultimate = signals_df.iloc[-2]

            sell_alert = False
            buy_alert = False

            if penultimate["buySignal"]:
                buy_alert = True
            if penultimate["sellSignal"]:
                sell_alert = True

            if not buy_alert and not sell_alert:
                return None

            # Report the exact value (last candle for debug)
            return {
                'symbol': symbol,
                'signal_type': 'BUY' if buy_alert else 'SELL',
                'wt1': round(penultimate['wt1'], 1),
                'wt2': round(penultimate['wt2'], 1),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '2h'
            }

        except Exception as e:
            print(f"⚠️ CipherB analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
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
        print("🎯 CIPHERB 2H ANALYSIS STARTING")
        print("="*50)
        start_time = datetime.utcnow()
        coins = self.load_cipherb_dataset()
        if not coins:
            return
        print(f"📊 Analyzing {len(coins)} CipherB coins (≥$100M cap, ≥$10M vol)")
        print("⚡ STRICT TradingView Signal: Only plotshape matches on 2H!")
        signals = self.process_coins_parallel(coins)
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
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    analyzer = CipherB2HAnalyzer(config)
    analyzer.run_cipherb_analysis()

if __name__ == '__main__':
    main()
