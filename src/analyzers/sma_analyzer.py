#!/usr/bin/env python3
"""
SMA 2H Analyzer - Using Base Class
"""
import os
import sys
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.base_analyzer import BaseAnalyzer
from src.indicators.sma import detect_sma_signals
from src.alerts.sma_telegram import SMATelegramSender

class SMA2HAnalyzer(BaseAnalyzer):
    def __init__(self, config: Dict):
        super().__init__(config, 'sma')
        self.telegram_sender = SMATelegramSender(config)
        
    def _analyze_indicator(self, ohlcv_data: Dict, coin_data: Dict, exchange_used: str) -> Optional[Dict]:
        """SMA-specific analysis"""
        try:
            # Detect SMA signals
            signal_result = detect_sma_signals(ohlcv_data, self.system_config)
            if not signal_result:
                return None

            return {
                'symbol': coin_data['symbol'],
                'signal_type': signal_result.get('signal_type', 'SMA_SIGNAL'),
                'sma_data': signal_result,
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '2h'
            }

        except Exception as e:
            self.logger.error(f"SMA indicator analysis failed: {str(e)}")
            return None

    def _send_alerts(self, signals: list) -> bool:
        """Send SMA alerts"""
        return self.telegram_sender.send_sma_batch_alert(signals, {
            'total_crossovers': sum(1 for s in signals if 'crossover' in s.get('signal_type', '')),
            'total_ranging': sum(1 for s in signals if 'ranging' in s.get('signal_type', ''))
        })

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = SMA2HAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
