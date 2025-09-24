#!/usr/bin/env python3
"""
BBW 15M Analyzer - Using Base Class
"""
import os
import sys
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.base_analyzer import BaseAnalyzer
from src.indicators.bbw import BBWIndicator, detect_bbw_squeeze_signals
from src.alerts.bbw_telegram import BBWTelegramSender

class BBW15MAnalyzer(BaseAnalyzer):
    def __init__(self, config: Dict):
        super().__init__(config, 'bbw')
        self.telegram_sender = BBWTelegramSender(config)
        self.bbw_indicator = BBWIndicator(config['bbw_config'])
        
    def _analyze_indicator(self, ohlcv_data: Dict, coin_data: Dict, exchange_used: str) -> Optional[Dict]:
        """BBW-specific analysis"""
        try:
            # Detect BBW squeeze
            squeeze_detected, bbw_result = detect_bbw_squeeze_signals(ohlcv_data, self.system_config)
            if not squeeze_detected:
                return None

            # First-touch deduplication
            first_touch_detected, alert_data = self.bbw_indicator.detect_first_touch_contraction(
                coin_data['symbol'], bbw_result['bbw_data']
            )
            if not first_touch_detected:
                return None

            return {
                'symbol': coin_data['symbol'],
                'signal_type': 'BBW_SQUEEZE',
                'bbw_value': alert_data['bbw_value'],
                'contraction_line': alert_data['contraction_line'],
                'consecutive_confirmed': alert_data['consecutive_confirmed'],
                'coin_data': coin_data,
                'exchange_used': exchange_used,
                'timeframe': '15m'
            }

        except Exception as e:
            self.logger.error(f"BBW indicator analysis failed: {str(e)}")
            return None

    def _send_alerts(self, signals: list) -> bool:
        """Send BBW alerts"""
        return self.telegram_sender.send_bbw_batch_alert(signals)

def main():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    analyzer = BBW15MAnalyzer(config)
    analyzer.run_analysis()

if __name__ == '__main__':
    main()
