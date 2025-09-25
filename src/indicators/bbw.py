"""
BBW Indicator - Simple & Bulletproof with Persistent Cache
"""
import json
import os
import time
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple

getcontext().prec = 50

class BBWIndicator:
    def __init__(self):
        self.length = 20
        self.mult = 2.0  
        self.expansion_length = 125
        self.contraction_length = 125
        # IMPORTANT: Use absolute path for cache persistence
        self.cache_file = os.path.abspath("cache/bbw_squeeze_alerts.json")

    def calculate_sma(self, data: List[float], period: int) -> List[float]:
        """High-precision SMA"""
        result = []
        for i in range(len(data)):
            if i < period - 1:
                result.append(float('nan'))
            else:
                sum_val = sum(Decimal(str(data[j])) for j in range(i - period + 1, i + 1))
                result.append(float(sum_val / period))
        return result

    def calculate_stdev(self, data: List[float], period: int) -> List[float]:
        """High-precision standard deviation (sample with N-1)"""
        result = []
        sma_vals = self.calculate_sma(data, period)
        
        for i in range(len(data)):
            if i < period - 1:
                result.append(float('nan'))
            else:
                mean = Decimal(str(sma_vals[i]))
                variance_sum = sum((Decimal(str(data[j])) - mean) ** 2 
                                 for j in range(i - period + 1, i + 1))
                variance = variance_sum / (period - 1)  # Sample std dev
                result.append(float(variance.sqrt()))
        return result

    def calculate_highest(self, data: List[float], period: int) -> List[float]:
        """Rolling maximum"""
        result = []
        for i in range(len(data)):
            start_idx = max(0, i - period + 1)
            window = data[start_idx:i + 1]
            result.append(max(window))
        return result

    def calculate_lowest(self, data: List[float], period: int) -> List[float]:
        """Rolling minimum"""
        result = []
        for i in range(len(data)):
            start_idx = max(0, i - period + 1)
            window = data[start_idx:i + 1]
            result.append(min(window))
        return result

    def calculate_bbw(self, closes: List[float]) -> Tuple[float, float, float]:
        """Calculate current BBW and dynamic lines"""
        if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
            return 0, 0, 0

        # Calculate BBW
        basis = self.calculate_sma(closes, self.length)
        stdev = self.calculate_stdev(closes, self.length)
        
        bbw_values = []
        for i in range(len(closes)):
            if str(basis[i]) == 'nan' or str(stdev[i]) == 'nan' or basis[i] == 0:
                bbw_values.append(0)
            else:
                dev = self.mult * stdev[i]
                upper = basis[i] + dev
                lower = basis[i] - dev
                bbw_val = ((upper - lower) / basis[i]) * 100
                bbw_values.append(bbw_val)

        # Calculate dynamic lines
        valid_bbw = [x for x in bbw_values if x > 0]
        if len(valid_bbw) < max(self.expansion_length, self.contraction_length):
            return 0, 0, 0
            
        highest_expansion = self.calculate_highest(valid_bbw, self.expansion_length)
        lowest_contraction = self.calculate_lowest(valid_bbw, self.contraction_length)

        return bbw_values[-1], highest_expansion[-1], lowest_contraction[-1]

    def load_cache(self) -> Dict:
        """Load persistent cache"""
        try:
            # Ensure cache directory exists
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache = json.load(f)
                    print(f"📁 Loaded cache with {len(cache)} entries")
                    return cache
        except Exception as e:
            print(f"❌ Cache load error: {e}")
        
        print("📁 Creating new cache")
        return {}

    def save_cache(self, cache_data: Dict):
        """Save persistent cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            print(f"💾 Cache saved with {len(cache_data)} entries")
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def check_squeeze_alert(self, symbol: str, current_bbw: float, lowest_contraction: float) -> Dict:
        """
        BULLETPROOF squeeze detection with persistent cache:
        
        1. Alert ONLY on FIRST entry into squeeze zone
        2. NO alerts while staying in zone (across multiple runs)
        3. 20H reminder after prolonged squeeze
        4. New alert only after complete exit and re-entry
        """
        current_time = time.time()
        
        if current_bbw <= 0 or lowest_contraction <= 0:
            return {
                'send_alert': False,
                'alert_type': None,
                'range_top': 0
            }

        # Calculate squeeze zone
        zone_bottom = lowest_contraction
        zone_top = lowest_contraction * 2.0  # 100% above contraction
        is_in_zone = zone_bottom <= current_bbw <= zone_top

        # Load cache
        cache = self.load_cache()
        cache_key = f"{symbol}_squeeze"

        # Get previous state
        if cache_key in cache:
            was_in_zone = cache[cache_key].get('in_zone', False)
            zone_entry_time = cache[cache_key].get('entry_time', current_time)
            reminder_sent = cache[cache_key].get('reminder_sent', False)
        else:
            was_in_zone = False
            zone_entry_time = current_time
            reminder_sent = False

        result = {
            'send_alert': False,
            'alert_type': None,
            'range_top': zone_top
        }

        if is_in_zone:
            if not was_in_zone:
                # 🚨 FIRST ENTRY - SEND ALERT
                result['send_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                
                cache[cache_key] = {
                    'in_zone': True,
                    'entry_time': current_time,
                    'reminder_sent': False
                }
                
                print(f"🚨 FIRST ENTRY: {symbol} BBW {current_bbw:.2f} entered zone [{zone_bottom:.2f}-{zone_top:.2f}]")
                
            else:
                # Already in zone - check for 20H reminder
                hours_in_zone = (current_time - zone_entry_time) / 3600
                
                if hours_in_zone >= 20 and not reminder_sent:
                    result['send_alert'] = True
                    result['alert_type'] = 'EXTENDED SQUEEZE'
                    
                    cache[cache_key]['reminder_sent'] = True
                    
                    print(f"🔔 REMINDER: {symbol} in zone for {hours_in_zone:.1f} hours")
                else:
                    print(f"📍 STAY IN ZONE: {symbol} (in zone {hours_in_zone:.1f}h) - NO ALERT")
        else:
            if was_in_zone:
                # 🚪 EXIT ZONE - Reset for future alerts
                cache[cache_key] = {
                    'in_zone': False,
                    'entry_time': 0,
                    'reminder_sent': False
                }
                print(f"🚪 ZONE EXIT: {symbol} BBW {current_bbw:.2f} - Ready for new alerts")
            else:
                print(f"🌐 OUTSIDE ZONE: {symbol} BBW {current_bbw:.2f}")

        # SAVE CACHE (Critical!)
        self.save_cache(cache)
        return result

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main analysis function"""
        try:
            closes = ohlcv_data['close']
            current_bbw, highest_expansion, lowest_contraction = self.calculate_bbw(closes)
            
            if current_bbw <= 0:
                return {'send_alert': False, 'error': 'Invalid BBW calculation'}

            # Check for squeeze alert
            alert_result = self.check_squeeze_alert(symbol, current_bbw, lowest_contraction)
            
            return {
                'send_alert': alert_result['send_alert'],
                'alert_type': alert_result['alert_type'],
                'bbw': current_bbw,
                'lowest_contraction': lowest_contraction,
                'highest_expansion': highest_expansion,
                'range_top': alert_result['range_top']
            }
            
        except Exception as e:
            print(f"❌ BBW analysis error for {symbol}: {e}")
            return {'send_alert': False, 'error': str(e)}
