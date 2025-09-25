"""
BBW Indicator - New Deduplication Logic with 20H Reminders
"""
import json
import os
import time
from typing import Dict, List, Tuple

class BBWIndicator:
    def __init__(self):
        self.length = 20
        self.mult = 2.0
        self.expansion_length = 125
        self.contraction_length = 125
        self.cache_file = "cache/bbw_alerts.json"

    def calculate_bbw(self, closes: List[float]) -> Tuple[List[float], float, float]:
        """Calculate BBW exactly like Pine Script"""
        if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
            return [], 0, 0
        
        bbw_values = []
        
        for i in range(len(closes)):
            if i < self.length - 1:
                bbw_values.append(0)
                continue
            
            # SMA (basis)
            basis = sum(closes[i - self.length + 1:i + 1]) / self.length
            
            # Standard deviation
            variance = sum((closes[j] - basis) ** 2 for j in range(i - self.length + 1, i + 1)) / self.length
            stddev = variance ** 0.5
            
            # Bollinger Bands
            upper = basis + (self.mult * stddev)
            lower = basis - (self.mult * stddev)
            
            # BBW calculation
            if basis != 0:
                bbw = ((upper - lower) / basis) * 100
            else:
                bbw = 0
            
            bbw_values.append(bbw)
        
        # Calculate highest expansion and lowest contraction
        current_bbw = bbw_values[-1] if bbw_values else 0
        
        expansion_lookback = min(self.expansion_length, len(bbw_values))
        highest_expansion = max(bbw_values[-expansion_lookback:]) if expansion_lookback > 0 else 0
        
        contraction_lookback = min(self.contraction_length, len(bbw_values))
        lowest_contraction = min(bbw_values[-contraction_lookback:]) if contraction_lookback > 0 else 0
        
        return bbw_values, highest_expansion, lowest_contraction

    def load_cache(self) -> Dict:
        """Load alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_cache(self, cache_data: Dict):
        """Save alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f)
        except:
            pass

    def check_squeeze_status(self, symbol: str, current_bbw: float, lowest_contraction: float) -> Dict:
        """
        NEW DEDUPLICATION LOGIC:
        - Alert when BBW first enters squeeze range (contraction to 75% above)
        - Track entry time and status
        - Send 20-hour reminder if still in squeeze
        """
        # Calculate squeeze threshold (75% above contraction)
        squeeze_threshold = lowest_contraction * 1.75  # Exact 75% above
        
        # Check if BBW is in squeeze range
        is_in_squeeze = lowest_contraction <= current_bbw <= squeeze_threshold
        
        # Load cache
        cache = self.load_cache()
        current_time = time.time()
        
        # Initialize tracking for this symbol
        if symbol not in cache:
            cache[symbol] = {
                'is_in_squeeze': False,
                'first_entry_time': None,
                'last_alert_time': None,
                'reminder_sent': False
            }
        
        symbol_data = cache[symbol]
        
        result = {
            'should_alert': False,
            'alert_type': 'FIRST ENTRY',
            'should_remind': False,
            'hours_in_squeeze': 0
        }
        
        if is_in_squeeze:
            # BBW is currently in squeeze range
            if not symbol_data['is_in_squeeze']:
                # FIRST ENTRY into squeeze range
                symbol_data['is_in_squeeze'] = True
                symbol_data['first_entry_time'] = current_time
                symbol_data['last_alert_time'] = current_time
                symbol_data['reminder_sent'] = False
                
                result['should_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                
                print(f"BBW first squeeze entry: {symbol} BBW:{current_bbw:.2f} in range [{lowest_contraction:.2f} - {squeeze_threshold:.2f}]")
            
            else:
                # Already in squeeze - check for 20-hour reminder
                time_in_squeeze = current_time - symbol_data['first_entry_time']
                hours_in_squeeze = time_in_squeeze / 3600
                
                if hours_in_squeeze >= 20 and not symbol_data['reminder_sent']:
                    # Send 20-hour reminder
                    result['should_remind'] = True
                    result['hours_in_squeeze'] = int(hours_in_squeeze)
                    
                    symbol_data['reminder_sent'] = True
                    symbol_data['last_alert_time'] = current_time
                    
                    print(f"BBW 20H reminder: {symbol} still in squeeze for {hours_in_squeeze:.1f} hours")
        
        else:
            # BBW has exited squeeze range
            if symbol_data['is_in_squeeze']:
                # Just exited - reset tracking
                symbol_data['is_in_squeeze'] = False
                symbol_data['first_entry_time'] = None
                symbol_data['reminder_sent'] = False
                
                print(f"BBW exit squeeze: {symbol} BBW:{current_bbw:.2f} exited range")
        
        # Save updated cache
        cache[symbol] = symbol_data
        self.save_cache(cache)
        
        return result

    def calculate_bbw_signals(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main BBW calculation with new deduplication logic"""
        try:
            closes = ohlcv_data['close']
            bbw_values, highest_expansion, lowest_contraction = self.calculate_bbw(closes)
            
            if not bbw_values:
                return {'squeeze_signal': False, 'error': 'Insufficient data'}
            
            current_bbw = bbw_values[-1]
            squeeze_threshold = lowest_contraction * 1.75
            
            # Check squeeze status with new logic
            squeeze_status = self.check_squeeze_status(symbol, current_bbw, lowest_contraction)
            
            return {
                'squeeze_signal': squeeze_status['should_alert'],
                'reminder_signal': squeeze_status['should_remind'],
                'alert_type': squeeze_status['alert_type'],
                'hours_in_squeeze': squeeze_status['hours_in_squeeze'],
                'bbw': current_bbw,
                'lowest_contraction': lowest_contraction,
                'highest_expansion': highest_expansion,
                'squeeze_threshold': squeeze_threshold
            }
            
        except Exception as e:
            return {'squeeze_signal': False, 'error': str(e)}
