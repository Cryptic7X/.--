"""
BBW Indicator - 100% Range Logic with Smart Deduplication & 20H Reminders
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
        """Load BBW alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_cache(self, cache_data: Dict):
        """Save BBW alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except:
            pass

    def check_squeeze_entry_and_reminders(self, symbol: str, current_bbw: float, lowest_contraction: float) -> Dict:
        """
        Smart deduplication + 20H reminders logic:
        
        1. Alert when BBW FIRST enters range [contraction_line to 100% above]
        2. No more alerts while BBW stays in range
        3. After 20H in range, send reminder alert
        4. New first-entry alert only after BBW exits and re-enters
        """
        current_time = time.time()
        
        # Calculate 100% range (100% above contraction)
        range_top = lowest_contraction * 2.0  # 100% above contraction
        
        # Check if BBW is in squeeze range
        is_in_squeeze_range = lowest_contraction <= current_bbw <= range_top
        
        # Load cache
        cache = self.load_cache()
        cache_key = f"{symbol}_squeeze"
        
        result = {
            'first_entry_alert': False,
            'reminder_alert': False,
            'alert_type': None,
            'range_top': range_top
        }
        
        if is_in_squeeze_range:
            # BBW is currently in squeeze range
            if cache_key in cache:
                # We've seen this symbol before
                entry_time = cache[cache_key].get('entry_time', current_time)
                last_alert_time = cache[cache_key].get('last_alert_time', 0)
                was_in_range = cache[cache_key].get('was_in_range', False)
                reminder_sent = cache[cache_key].get('reminder_sent', False)
                
                if not was_in_range:
                    # BBW just re-entered squeeze range - FIRST ENTRY ALERT
                    result['first_entry_alert'] = True
                    result['alert_type'] = 'FIRST ENTRY'
                    
                    cache[cache_key] = {
                        'entry_time': current_time,
                        'last_alert_time': current_time,
                        'was_in_range': True,
                        'reminder_sent': False
                    }
                else:
                    # BBW has been in range for some time
                    time_in_range_hours = (current_time - entry_time) / 3600
                    
                    if time_in_range_hours >= 20 and not reminder_sent:
                        # Send 20H reminder alert
                        result['reminder_alert'] = True
                        result['alert_type'] = 'EXTENDED SQUEEZE'
                        
                        cache[cache_key]['reminder_sent'] = True
                        cache[cache_key]['last_alert_time'] = current_time
                    
                    # Update current state
                    cache[cache_key]['was_in_range'] = True
            else:
                # First time seeing this symbol in squeeze range - FIRST ENTRY ALERT
                result['first_entry_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                
                cache[cache_key] = {
                    'entry_time': current_time,
                    'last_alert_time': current_time,
                    'was_in_range': True,
                    'reminder_sent': False
                }
        else:
            # BBW is outside squeeze range
            if cache_key in cache:
                # Mark that we're no longer in range (reset for next entry)
                cache[cache_key]['was_in_range'] = False
                cache[cache_key]['reminder_sent'] = False
        
        # Save updated cache
        self.save_cache(cache)
        return result

    def calculate_bbw_signals(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main BBW calculation with smart alerts"""
        try:
            closes = ohlcv_data['close']
            bbw_values, highest_expansion, lowest_contraction = self.calculate_bbw(closes)
            
            if not bbw_values:
                return {'squeeze_signal': False, 'error': 'Insufficient data'}
            
            current_bbw = bbw_values[-1]
            
            # Check for squeeze entry and reminders
            alert_result = self.check_squeeze_entry_and_reminders(symbol, current_bbw, lowest_contraction)
            
            # Determine if any alert should be sent
            send_alert = alert_result['first_entry_alert'] or alert_result['reminder_alert']
            
            if send_alert:
                print(f"BBW {alert_result['alert_type']}: {symbol} BBW:{current_bbw:.2f} in range [{lowest_contraction:.2f} - {alert_result['range_top']:.2f}]")
            
            return {
                'squeeze_signal': send_alert,
                'alert_type': alert_result['alert_type'],
                'bbw': current_bbw,
                'lowest_contraction': lowest_contraction,
                'highest_expansion': highest_expansion,
                'range_top': alert_result['range_top']
            }
            
        except Exception as e:
            return {'squeeze_signal': False, 'error': str(e)}
