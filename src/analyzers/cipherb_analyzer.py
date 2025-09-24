#!/usr/bin/env python3
"""
CipherB 2H Analyzer - Using Base Class
"""
import os
import sys
import pandas as pd
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.base_analyzer import BaseAnalyzer
from src.indicators.cipherb import detect_exact_cipherb_signals
from src.alerts.cipherb_telegram import CipherBTelegramSender

class CipherB2HAnalyzer(BaseAnalyzer):
    def __init__(self, config: Dict):
        super().__init__(config, 'cipherb')
        self.telegram_sender = CipherBTelegramSender(config)
        
    def _analyze_indicator(self, ohlcv_data: Dict, coin_data: Dict, exchange_used: str) -> Optional[Dict]:
        """CipherB-specific analysis"""
        try:
            # Create DataFrame
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

            # Run CipherB analysis
            signals_df = detect_exact_cipherb_signals(df, {
                'wt_channel_len': 9, 
                'wt_average_len': 12,
                'wt_ma_len': 3,
                'oversold_threshold': -60,
                'overbought_threshold': 60
            })
            
            if signals_df.empty:
                return None

            # Check penultimate candle for signals (TradingView behavior)
            penultimate = signals_df.iloc[-2]
            
            if not (penultimate["buySignal"] or penultimate["sellSignal"]):
                return None

            return {
                'symbol': coin_data['symbol'],
                'signal_type': 'BUY' if penultimate["buySignal"] else 'SELL',
                'wt1': round(penultimate['wt1'], 1),
                'wt2': round(penultimate['wt2'], 1),
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '2h'
            }

        except Exception as e:
            self.logger.error(f"CipherB indicator analysis failed: {str(e)}")
            return None

    def _send_alerts(self, signals: list) -> bool:
        """Send CipherB alerts"""
        return self.telegram_sender.send_cipherb_batch_alert(signals)

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = CipherB2HAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
