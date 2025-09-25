"""
BBW Indicator - CORRECTED Pine Script Implementation
Exact match to your Pine Script logic
"""
import json
import os
import time
from typing import Dict, List, Tuple
import math

class BBWIndicator:
    def __init__(self):
        # Exact Pine Script parameters
        self.length = 20
        self.mult = 2.0
        self.expansion_length = 125
        self.contraction_length = 125
        self.cache_file = "cache/bbw_alerts.json"

    def calculate_sma(self, data: List[float], period: int) -> float:
        """Calculate SMA for a single period - matches ta.sma()"""
        if len(data) < period:
            return 0.0
        return sum(data[-period:]) / period

    def calculate_stdev(self, data: List[float], period: int) -> float:
        """Calculate standard deviation - matches ta.stdev()"""
        if len(data) < period:
            return 0.0
        
        recent_data = data[-period:]
        mean = sum(recent_data) / period
        variance = sum((x - mean) ** 2 for x in recent_data) / period
        return math.sqrt(variance)

    def calculate_bbw_series(self, closes: List[float]) -> List[float]:
        """
        Calculate BBW series exactly like Pine Script
        bbw = ((upper - lower) / basis) * 100
        """
        if len(closes) < self.length:
            return []
        
        bbw_values = []
        
        # Calculate BBW for each point where we have enough data
        for i in range(self.length - 1, len(closes)):
            # Get data up to current point
            data_slice = closes[:i + 1]
            
            # Calculate basis (SMA) - ta.sma(src, length)
            basis = self.calculate_sma(data_slice, self.length)
            
            # Calculate deviation - mult * ta.stdev(src, length)  
            dev = self.mult * self.calculate_stdev(data_slice, self.length)
            
            # Calculate Bollinger Bands
            upper = basis + dev
            lower = basis - dev
            
            # Calculate BBW - ((upper - lower) / basis) * 100
            if basis != 0:
                bbw = ((upper - lower) / basis) * 100
            else:
                bbw = 0
            
            bbw_values.append(bbw)
        
        return bbw_values

    def calculate_dynamic_levels(self, bbw_values: List[float]) -> Tuple[float, float]:
        """
        Calculate dynamic levels exactly like Pine Script:
        - ta.highest(bbw, expansionLengthInput) 
        - ta.lowest(bbw, contractionLengthInput)
        """
        if not bbw_values:
            return 0.0, 0.0
        
        # Highest expansion - ta.highest(bbw, 125)
        expansion_lookback = min(self.expansion_length, len(bbw_values))
        highest_expansion = max(bbw_values[-expansion_lookback:]) if expansion_lookback > 0 else 0.0
        
        # Lowest contraction - ta.lowest(bbw, 125)
        contraction_lookback = min(self.contraction_length, len(bbw_values))
        lowest_contraction = min(bbw_values[-contraction_lookback:]) if contraction_lookback > 0 else 0.0
        
        return highest_expansion, lowest_contraction

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
        Smart deduplication + 20H reminders:
        Range: From lowest_contraction to 100% above (lowest_contraction * 2.0)
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
        """Main BBW calculation with CORRECTED Pine Script logic"""
        try:
            closes = ohlcv_data['close']
            
            if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
                return {'squeeze_signal': False, 'error': 'Insufficient data'}

            # Calculate BBW series exactly like Pine Script
            bbw_values = self.calculate_bbw_series(closes)
            
            if not bbw_values:
                return {'squeeze_signal': False, 'error': 'No BBW values calculated'}
            
            # Get current BBW
            current_bbw = bbw_values[-1]
            
            # Calculate dynamic levels exactly like Pine Script
            highest_expansion, lowest_contraction = self.calculate_dynamic_levels(bbw_values)
            
            # Check for squeeze entry and reminders
            alert_result = self.check_squeeze_entry_and_reminders(symbol, current_bbw, lowest_contraction)
            
            # Determine if any alert should be sent
            send_alert = alert_result['first_entry_alert'] or alert_result['reminder_alert']
            
            if send_alert:
                print(f"BBW {alert_result['alert_type']}: {symbol} BBW:{current_bbw:.2f} Range:[{lowest_contraction:.2f} - {alert_result['range_top']:.2f}]")
            
            return {
                'squeeze_signal': send_alert,
                'alert_type': alert_result['alert_type'],
                'bbw': current_bbw,
                'lowest_contraction': lowest_contraction,
                'highest_expansion': highest_expansion,
                'range_top': alert_result['range_top']
            }
            
        except Exception as e:
            print(f"BBW calculation error for {symbol}: {e}")
            return {'squeeze_signal': False, 'error': str(e)}
