"""
BBW Indicator - EXACT Pine Script Match
Fixed standard deviation calculation to match Pine Script exactly
"""
import json
import os
import time
import math
from typing import Dict, List, Tuple

class BBWIndicator:
    def __init__(self):
        # EXACT Pine Script parameters
        self.length = 20
        self.mult = 2.0
        self.expansion_length = 125
        self.contraction_length = 125
        self.cache_file = "cache/bbw_alerts.json"

    def calculate_sma(self, data: List[float], period: int) -> List[float]:
        """Calculate SMA exactly like Pine Script ta.sma()"""
        sma_values = []
        
        for i in range(len(data)):
            if i < period - 1:
                sma_values.append(0.0)
            else:
                sum_values = sum(data[i - period + 1:i + 1])
                sma_values.append(sum_values / period)
        
        return sma_values

    def calculate_stdev_pinescript(self, data: List[float], period: int) -> List[float]:
        """
        Calculate standard deviation EXACTLY like Pine Script ta.stdev()
        Pine Script uses sample standard deviation (Bessel's correction) with N-1
        """
        stdev_values = []
        sma_values = self.calculate_sma(data, period)
        
        for i in range(len(data)):
            if i < period - 1:
                stdev_values.append(0.0)
            else:
                mean = sma_values[i]
                variance_sum = 0.0
                
                # Calculate variance
                for j in range(i - period + 1, i + 1):
                    variance_sum += (data[j] - mean) ** 2
                
                # CRITICAL: Pine Script uses sample standard deviation (N-1)
                variance = variance_sum / (period - 1) if period > 1 else 0
                stdev = math.sqrt(variance)
                stdev_values.append(stdev)
        
        return stdev_values

    def calculate_highest(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling highest exactly like Pine Script ta.highest()"""
        highest_values = []
        
        for i in range(len(data)):
            if i < period - 1:
                if i >= 0:
                    highest_values.append(max(data[0:i + 1]))
                else:
                    highest_values.append(data[0] if data else 0.0)
            else:
                window = data[i - period + 1:i + 1]
                highest_values.append(max(window))
        
        return highest_values

    def calculate_lowest(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling lowest exactly like Pine Script ta.lowest()"""
        lowest_values = []
        
        for i in range(len(data)):
            if i < period - 1:
                if i >= 0:
                    lowest_values.append(min(data[0:i + 1]))
                else:
                    lowest_values.append(data[0] if data else 0.0)
            else:
                window = data[i - period + 1:i + 1]
                lowest_values.append(min(window))
        
        return lowest_values

    def calculate_bbw_exact_pinescript(self, closes: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """
        EXACT Pine Script BBW calculation with CORRECTED standard deviation:
        
        basis = ta.sma(src, length)
        dev   = mult * ta.stdev(src, length)  <-- Uses sample stdev (N-1)
        upper = basis + dev
        lower = basis - dev
        bbw   = ((upper - lower) / basis) * 100
        """
        if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
            return [], [], []
        
        # Step 1: Calculate basis (SMA)
        basis = self.calculate_sma(closes, self.length)
        
        # Step 2: Calculate dev (CORRECTED standard deviation with N-1)
        stdev = self.calculate_stdev_pinescript(closes, self.length)
        dev = [self.mult * s for s in stdev]
        
        # Step 3: Calculate Bollinger Bands
        upper = [basis[i] + dev[i] for i in range(len(basis))]
        lower = [basis[i] - dev[i] for i in range(len(basis))]
        
        # Step 4: Calculate BBW
        bbw = []
        for i in range(len(upper)):
            if basis[i] != 0:
                bbw_val = ((upper[i] - lower[i]) / basis[i]) * 100
            else:
                bbw_val = 0.0
            bbw.append(bbw_val)
        
        # Step 5: Calculate dynamic lines
        highest_expansion = self.calculate_highest(bbw, self.expansion_length)
        lowest_contraction = self.calculate_lowest(bbw, self.contraction_length)
        
        return bbw, highest_expansion, lowest_contraction

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
        """100% Range + 20H reminder logic"""
        current_time = time.time()
        
        # 100% above contraction 
        range_top = lowest_contraction * 2.0
        
        # Check if in squeeze range
        is_in_squeeze_range = lowest_contraction <= current_bbw <= range_top
        
        cache = self.load_cache()
        cache_key = f"{symbol}_squeeze"
        
        result = {
            'first_entry_alert': False,
            'reminder_alert': False,
            'alert_type': None,
            'range_top': range_top
        }
        
        if is_in_squeeze_range:
            if cache_key in cache:
                entry_time = cache[cache_key].get('entry_time', current_time)
                was_in_range = cache[cache_key].get('was_in_range', False)
                reminder_sent = cache[cache_key].get('reminder_sent', False)
                
                if not was_in_range:
                    result['first_entry_alert'] = True
                    result['alert_type'] = 'FIRST ENTRY'
                    
                    cache[cache_key] = {
                        'entry_time': current_time,
                        'last_alert_time': current_time,
                        'was_in_range': True,
                        'reminder_sent': False
                    }
                else:
                    time_in_range_hours = (current_time - entry_time) / 3600
                    
                    if time_in_range_hours >= 20 and not reminder_sent:
                        result['reminder_alert'] = True
                        result['alert_type'] = 'EXTENDED SQUEEZE'
                        
                        cache[cache_key]['reminder_sent'] = True
                        cache[cache_key]['last_alert_time'] = current_time
                    
                    cache[cache_key]['was_in_range'] = True
            else:
                result['first_entry_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                
                cache[cache_key] = {
                    'entry_time': current_time,
                    'last_alert_time': current_time,
                    'was_in_range': True,
                    'reminder_sent': False
                }
        else:
            if cache_key in cache:
                cache[cache_key]['was_in_range'] = False
                cache[cache_key]['reminder_sent'] = False
        
        self.save_cache(cache)
        return result

    def calculate_bbw_signals(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main BBW calculation with CORRECTED Pine Script logic"""
        try:
            closes = ohlcv_data['close']
            
            if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
                return {'squeeze_signal': False, 'error': 'Insufficient data'}

            # Use CORRECTED BBW calculation with proper standard deviation
            bbw_values, highest_expansion, lowest_contraction = self.calculate_bbw_exact_pinescript(closes)
            
            if not bbw_values:
                return {'squeeze_signal': False, 'error': 'BBW calculation failed'}
            
            current_bbw = bbw_values[-1]
            current_highest = highest_expansion[-1]
            current_lowest = lowest_contraction[-1]
            
            # Debug output - should now match TradingView
            print(f"FIXED {symbol}: BBW={current_bbw:.2f}, Highest={current_highest:.2f}, Lowest={current_lowest:.2f}")
            
            # Check squeeze logic
            alert_result = self.check_squeeze_entry_and_reminders(symbol, current_bbw, current_lowest)
            send_alert = alert_result['first_entry_alert'] or alert_result['reminder_alert']
            
            if send_alert:
                print(f"BBW {alert_result['alert_type']}: {symbol} BBW:{current_bbw:.2f} in range [{current_lowest:.2f} - {alert_result['range_top']:.2f}]")
            
            return {
                'squeeze_signal': send_alert,
                'alert_type': alert_result['alert_type'],
                'bbw': current_bbw,
                'lowest_contraction': current_lowest,
                'highest_expansion': current_highest,
                'range_top': alert_result['range_top']
            }
            
        except Exception as e:
            print(f"ERROR calculating BBW for {symbol}: {e}")
            return {'squeeze_signal': False, 'error': str(e)}
