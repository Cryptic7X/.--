#!/usr/bin/env python3
"""
CipherB Multi-Timeframe Analyzer (2H + 8H)
STRICT Pine Script Match with Multi-Timeframe Logic
FIXED: No alerts for new coins without 200 candles
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
from src.indicators.cipherb import CipherBMultiTimeframe
from src.alerts.cipherb_telegram import CipherBTelegramSender

class CipherBMultiAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = CipherBTelegramSender(config)
        self.cipherb_indicator = CipherBMultiTimeframe()

    def load_cipherb_dataset(self) -> List[Dict]:
        """Load CipherB coin dataset"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'cipherb_dataset.json')
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
            coins = data.get('coins', [])
            
        print(f"📊 Loaded {len(coins)} CipherB coins from cache")
        return coins

    def analyze_single_coin_2h(self, coin_data: Dict) -> Optional[Dict]:
        """Analyze single coin for 2H signals"""
        symbol = coin_data['symbol']
        
        try:
            # Get 2H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '2h', limit=200
            )
            
            # FIXED: Strict check for new coins - must have close to 200 candles
            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 180:  # CHANGED: 25 → 180
                if ohlcv_data and len(ohlcv_data.get('timestamp', [])) < 180:
                    print(f"🚫 {symbol}: Only {len(ohlcv_data.get('timestamp', []))} candles - skipping (new coin)")
                return None
            
            # Create DataFrame for your indicator
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
            
            # FIXED: Double-check after DataFrame creation
            if len(df) < 180:  # CHANGED: 25 → 180
                print(f"🚫 {symbol}: DataFrame only has {len(df)} candles - skipping (new coin)")
                return None
            
            # Analyze 2H timeframe
            result_2h = self.cipherb_indicator.analyze_timeframe(df, '2h', symbol)
            
            if result_2h:
                result_2h['coin_data'] = coin_data
                result_2h['exchange_used'] = exchange_used
                result_2h['candle_count'] = len(df)  # Add candle count for debugging
                
            return result_2h
            
        except Exception as e:
            print(f"❌ CipherB 2H analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def analyze_single_coin_8h(self, symbol: str) -> Optional[Dict]:
        """Analyze single coin for 8H signals (only for monitoring coins)"""
        try:
            # Get 8H OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '8h', limit=100
            )
            
            # FIXED: Strict check for 8H data too - need at least 75 8H candles (600H = 25 days)
            if not ohlcv_data or len(ohlcv_data.get('timestamp', [])) < 75:  # CHANGED: 25 → 75
                if ohlcv_data and len(ohlcv_data.get('timestamp', [])) < 75:
                    print(f"🚫 {symbol}: Only {len(ohlcv_data.get('timestamp', []))} 8H candles - skipping")
                return None
            
            # Create DataFrame for your indicator
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
            
            # FIXED: Double-check after DataFrame creation
            if len(df) < 75:  # CHANGED: 25 → 75
                print(f"🚫 {symbol}: 8H DataFrame only has {len(df)} candles - skipping")
                return None
            
            # Analyze 8H timeframe
            return self.cipherb_indicator.analyze_timeframe(df, '8h', symbol)
            
        except Exception as e:
            print(f"❌ CipherB 8H analysis failed for {symbol}: {str(e)[:50]}")
            return None

    def get_monitoring_coins(self) -> List[str]:
        """Get list of coins currently in 8H monitoring mode"""
        cache = self.cipherb_indicator.load_cache()
        monitoring_coins = []
        
        for symbol, data in cache.items():
            if data.get('monitoring_8h', False):
                monitoring_coins.append(symbol)
                
        return monitoring_coins

    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """Process coins in parallel for 2H analysis"""
        max_workers = self.config.get('cipherb', {}).get('max_workers', 10)
        signals_2h = []
        
        print(f"🔄 Processing {len(coins)} coins with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {executor.submit(self.analyze_single_coin_2h, coin): coin for coin in coins}
            processed = 0
            
            for future in concurrent.futures.as_completed(future_to_coin):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals_2h.append(result)
                        print(f"📊 2H {result['signal_type']}: {result['symbol']} ({result['candle_count']} candles)")  # Show candle count
                        
                    if processed % 50 == 0:
                        print(f"📈 Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    print(f"❌ Error processing {coin['symbol']}: {str(e)[:30]}")
                    continue
        
        return signals_2h

    def run_cipherb_analysis(self):
        """Main multi-timeframe CipherB analysis"""
        print("🔵 CIPHERB MULTI-TIMEFRAME ANALYSIS STARTING")
        print("=" * 50)
        start_time = datetime.utcnow()
        
        # Step 1: Load coins and run 2H analysis
        coins = self.load_cipherb_dataset()
        if not coins:
            return
        
        print(f"📊 Analyzing {len(coins)} CipherB coins (≥500M cap, ≥10M vol)")
        print("🎯 2H + 8H Multi-Timeframe Analysis")
        print("🚫 Filtering: Minimum 180 2H candles (~15 days history)")  # ADDED
        
        # Step 2: Get 2H signals
        signals_2h = self.process_coins_parallel(coins)
        
        # Step 3: Get coins needing 8H confirmation
        monitoring_coins = self.get_monitoring_coins()
        print(f"📡 8H Monitoring: {len(monitoring_coins)} coins")
        
        # Step 4: Analyze 8H for monitoring coins only
        signals_8h = {}
        if monitoring_coins:
            print(f"🔍 Running 8H analysis for {len(monitoring_coins)} monitoring coins...")
            for symbol in monitoring_coins:
                result_8h = self.analyze_single_coin_8h(symbol)
                if result_8h:
                    signals_8h[symbol] = result_8h
                    print(f"📊 8H {result_8h['signal_type']}: {symbol}")
        
        # Step 5: Determine final alerts
        final_alerts = []
        for signal_2h in signals_2h:
            symbol = signal_2h['symbol']
            signal_8h = signals_8h.get(symbol)
            
            # Determine alert action
            alert_decision = self.cipherb_indicator.determine_alert_action(
                symbol, signal_2h, signal_8h
            )
            
            if alert_decision['send_alert']:
                alert_data = {
                    'symbol': symbol,
                    'alert_type': alert_decision['alert_type'],
                    'message_type': alert_decision['message_type'],
                    'signal_2h': alert_decision['signal_2h'],
                    'signal_8h': alert_decision.get('signal_8h'),
                    'coin_data': signal_2h['coin_data']
                }
                final_alerts.append(alert_data)
                
                print(f"✅ ALERT: {alert_data['message_type']} - {symbol} {alert_data['alert_type']}")
        
        # Step 6: Send alerts
        if final_alerts:
            success = self.telegram_sender.send_cipherb_multi_alerts(final_alerts)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            print("=" * 50)
            print("🔵 CIPHERB MULTI-TIMEFRAME ANALYSIS COMPLETE")
            print(f"📊 Signals Found: {len(final_alerts)}")
            print(f"📱 Alert Sent: {'✅ Yes' if success else '❌ Failed'}")
            print(f"⏱️  Processing Time: {processing_time:.1f}s")
            print(f"💰 Coins Processed: {len(coins)}")
            print(f"🔍 8H Monitoring: {len(monitoring_coins)}")
            print("=" * 50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            print(f"📭 No CipherB signals found in this cycle")
            print(f"⏱️  Processing Time: {processing_time:.1f}s")
            print(f"💰 Coins Analyzed: {len(coins)}")
            print(f"🔍 8H Monitoring: {len(monitoring_coins)}")

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = CipherBMultiAnalyzer(config)
    analyzer.run_cipherb_analysis()

if __name__ == '__main__':
    main()
