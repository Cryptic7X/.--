"""
BBW Indicator - DEBUGGING VERSION
Shows exactly what's happening with cache logic
"""
import json
import os
import time
import threading
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple

getcontext().prec = 50

class BBWIndicator:
    def __init__(self):
        self.length = 20
        self.mult = 2.0  
        self.expansion_length = 125
        self.contraction_length = 125
        
        # THREAD-SAFE cache system
        self.cache_file = os.path.abspath("cache/bbw_squeeze_alerts.json")
        self._cache_lock = threading.Lock()
        self._cache = None

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
                variance = variance_sum / (period - 1)
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
        """THREAD-SAFE cache loading with DEBUG"""
        with self._cache_lock:
            if self._cache is not None:
                return self._cache
                
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        content = f.read()
                        if content.strip():  # Check if file has content
                            self._cache = json.loads(content)
                            print(f"📁 LOADED CACHE: {len(self._cache)} entries from {self.cache_file}")
                            return self._cache
                        else:
                            print(f"📁 EMPTY CACHE FILE: {self.cache_file}")
                else:
                    print(f"📁 NO CACHE FILE EXISTS: {self.cache_file}")
            except Exception as e:
                print(f"❌ Cache load error: {e}")
            
            print(f"📁 CREATING NEW CACHE: {self.cache_file}")
            self._cache = {}
            return self._cache

    def save_cache(self, cache_data: Dict):
        """THREAD-SAFE cache saving with DEBUG"""
        with self._cache_lock:
            self._cache = cache_data
            
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                
                temp_file = self.cache_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                os.replace(temp_file, self.cache_file)
                print(f"💾 SAVED CACHE: {len(cache_data)} entries to {self.cache_file}")
                
                # DEBUG: Print cache content
                for key, value in cache_data.items():
                    print(f"   📋 {key}: in_zone={value.get('in_zone', False)}")
                
            except Exception as e:
                print(f"❌ Cache save error: {e}")

    def check_squeeze_alert(self, symbol: str, current_bbw: float, lowest_contraction: float) -> Dict:
        """DEBUGGING squeeze detection with verbose output"""
        current_time = time.time()
        
        if current_bbw <= 0 or lowest_contraction <= 0:
            print(f"❌ INVALID VALUES: {symbol} BBW={current_bbw} Contraction={lowest_contraction}")
            return {
                'send_alert': False,
                'alert_type': None,
                'range_top': 0
            }

        # Calculate squeeze zone
        zone_bottom = lowest_contraction
        zone_top = lowest_contraction * 2.0
        is_in_zone = zone_bottom <= current_bbw <= zone_top

        print(f"🔍 ANALYZING {symbol}:")
        print(f"   BBW: {current_bbw:.2f}")
        print(f"   Zone: [{zone_bottom:.2f} - {zone_top:.2f}]")
        print(f"   In Zone: {is_in_zone}")

        # Load cache with debugging
        cache = self.load_cache()
        cache_key = f"{symbol}_squeeze"

        # Check previous state
        if cache_key in cache:
            was_in_zone = cache[cache_key].get('in_zone', False)
            zone_entry_time = cache[cache_key].get('entry_time', current_time)
            reminder_sent = cache[cache_key].get('reminder_sent', False)
            
            print(f"   📋 CACHE FOUND: was_in_zone={was_in_zone}, entry_time={zone_entry_time}, reminder_sent={reminder_sent}")
        else:
            was_in_zone = False
            zone_entry_time = current_time
            reminder_sent = False
            
            print(f"   📋 NO CACHE: First time seeing {symbol}")

        result = {
            'send_alert': False,
            'alert_type': None,
            'range_top': zone_top
        }

        if is_in_zone:
            if not was_in_zone:
                # 🚨 FIRST ENTRY
                result['send_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                
                cache[cache_key] = {
                    'in_zone': True,
                    'entry_time': current_time,
                    'reminder_sent': False
                }
                
                print(f"🚨 FIRST ENTRY ALERT: {symbol} - SENDING ALERT")
                
            else:
                # Already in zone
                hours_in_zone = (current_time - zone_entry_time) / 3600
                
                if hours_in_zone >= 20 and not reminder_sent:
                    result['send_alert'] = True
                    result['alert_type'] = 'EXTENDED SQUEEZE'
                    
                    cache[cache_key]['reminder_sent'] = True
                    print(f"🔔 REMINDER ALERT: {symbol} - in zone for {hours_in_zone:.1f}h")
                else:
                    print(f"📍 STAY IN ZONE: {symbol} - in zone {hours_in_zone:.1f}h - NO ALERT")
        else:
            if was_in_zone:
                # Exit zone
                cache[cache_key] = {
                    'in_zone': False,
                    'entry_time': 0,
                    'reminder_sent': False
                }
                print(f"🚪 ZONE EXIT: {symbol} - reset for future alerts")
            else:
                print(f"🌐 OUTSIDE ZONE: {symbol} - no change")

        # Save cache
        self.save_cache(cache)
        return result

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main analysis function with debugging"""
        try:
            closes = ohlcv_data['close']
            current_bbw, highest_expansion, lowest_contraction = self.calculate_bbw(closes)
            
            if current_bbw <= 0:
                return {'send_alert': False, 'error': 'Invalid BBW calculation'}

            # Check for squeeze alert with full debugging
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
