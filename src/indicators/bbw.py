"""

BBW Indicator - GitHub-Persistent Cache

Commits cache to git repository to persist between GitHub Action runs

"""

import json
import os
import time
import subprocess
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
        
        # GitHub-persistent cache
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
        """High-precision standard deviation (population with N) - CORRECTED for TradingView compatibility"""
        result = []
        sma_vals = self.calculate_sma(data, period)
        
        for i in range(len(data)):
            if i < period - 1:
                result.append(float('nan'))
            else:
                mean = Decimal(str(sma_vals[i]))
                variance_sum = sum((Decimal(str(data[j])) - mean) ** 2 
                                 for j in range(i - period + 1, i + 1))
                # FIXED: Use population standard deviation (N) instead of sample (N-1)
                variance = variance_sum / period
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
    
    def calculate_bbw(self, closes: List[float]) -> Tuple[float, float, float, List[float]]:
        """Calculate current BBW and dynamic lines with debugging - ENHANCED with BBW history"""
        if len(closes) < max(self.length, self.expansion_length, self.contraction_length):
            return 0, 0, 0, []
        
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
            return 0, 0, 0, []
        
        highest_expansion = self.calculate_highest(valid_bbw, self.expansion_length)
        lowest_contraction = self.calculate_lowest(valid_bbw, self.contraction_length)
        
        current_bbw = bbw_values[-1]
        current_highest = highest_expansion[-1]
        current_lowest = lowest_contraction[-1]
        
        return current_bbw, current_highest, current_lowest, bbw_values
    
    def commit_cache_to_git(self):
        """Commit cache file to git for persistence"""
        try:
            # Configure git
            subprocess.run(['git', 'config', 'user.name', 'BBW Cache Bot'], check=False)
            subprocess.run(['git', 'config', 'user.email', 'cache@bbw.bot'], check=False)
            
            # Add and commit cache file
            subprocess.run(['git', 'add', self.cache_file], check=False)
            result = subprocess.run(['git', 'commit', '-m', 'Update BBW cache'], 
                                  capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                print(f"✅ COMMITTED CACHE TO GIT")
                # Push to remote
                push_result = subprocess.run(['git', 'push'], capture_output=True, text=True, check=False)
                if push_result.returncode == 0:
                    print(f"✅ PUSHED CACHE TO REMOTE")
                else:
                    print(f"❌ Push failed: {push_result.stderr}")
            else:
                print(f"📝 No changes to commit")
                
        except Exception as e:
            print(f"❌ Git commit error: {e}")
    
    def load_cache(self) -> Dict:
        """Load cache from git repository"""
        with self._cache_lock:
            if self._cache is not None:
                return self._cache
            
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        content = f.read()
                        if content.strip():
                            self._cache = json.loads(content)
                            print(f"📁 LOADED CACHE: {len(self._cache)} entries from git")
                            
                            # Show cached symbols
                            cached_symbols = []
                            for key in self._cache.keys():
                                if '_squeeze' in key:
                                    symbol = key.replace('_squeeze', '')
                                    in_zone = self._cache[key].get('in_zone', False)
                                    cached_symbols.append(f"{symbol}:{in_zone}")
                            
                            print(f"📋 CACHED SYMBOLS: {', '.join(cached_symbols[:10])}" + 
                                  (f" + {len(cached_symbols)-10} more" if len(cached_symbols) > 10 else ""))
                            return self._cache
                            
            except Exception as e:
                print(f"❌ Cache load error: {e}")
            
            print(f"📁 CREATING NEW CACHE")
            self._cache = {}
            return self._cache
    
    def save_cache(self, cache_data: Dict):
        """Save cache and commit to git"""
        with self._cache_lock:
            self._cache = cache_data
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                with open(self.cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                print(f"💾 SAVED CACHE: {len(cache_data)} entries")
                
                # Commit to git for persistence
                self.commit_cache_to_git()
                
            except Exception as e:
                print(f"❌ Cache save error: {e}")
    
    def check_squeeze_alert(self, symbol: str, current_bbw: float, lowest_contraction: float, bbw_history: List[float]) -> Dict:
        """Squeeze detection with git-persistent cache - ENHANCED with entry direction detection"""
        current_time = time.time()
        
        if current_bbw <= 0 or lowest_contraction <= 0:
            return {
                'send_alert': False,
                'alert_type': None,
                'range_top': 0
            }
        
        # Calculate squeeze zone
        zone_bottom = lowest_contraction
        zone_top = lowest_contraction * 2.0
        is_in_zone = zone_bottom <= current_bbw <= zone_top
        
        # ENHANCED: Check entry direction - must enter from ABOVE
        entered_from_above = False
        if len(bbw_history) >= 2:
            previous_bbw = bbw_history[-2]
            entered_from_above = previous_bbw > zone_top and current_bbw <= zone_top
        
        # Load git-persistent cache
        cache = self.load_cache()
        cache_key = f"{symbol}_squeeze"
        
        # Check previous state
        if cache_key in cache:
            was_in_zone = cache[cache_key].get('in_zone', False)
            zone_entry_time = cache[cache_key].get('entry_time', current_time)
            reminder_sent = cache[cache_key].get('reminder_sent', False)
            candle_count = cache[cache_key].get('candle_count', 0)  # ENHANCED: Track candles
        else:
            was_in_zone = False
            zone_entry_time = current_time
            reminder_sent = False
            candle_count = 0
        
        result = {
            'send_alert': False,
            'alert_type': None,
            'range_top': zone_top
        }
        
        if is_in_zone:
            if not was_in_zone and entered_from_above:  # FIXED: Only alert on entry from ABOVE
                # 🚨 FIRST ENTRY FROM ABOVE
                result['send_alert'] = True
                result['alert_type'] = 'FIRST ENTRY'
                cache[cache_key] = {
                    'in_zone': True,
                    'entry_time': current_time,
                    'reminder_sent': False,
                    'candle_count': 1  # Start counting candles
                }
                
                print(f"🚨 FIRST ENTRY FROM ABOVE: {symbol} BBW {current_bbw:.2f} in zone [{zone_bottom:.2f}-{zone_top:.2f}]")
                
            elif was_in_zone:
                # Already in zone - increment candle count and check for 20-hour reminder
                new_candle_count = candle_count + 1
                hours_in_zone = new_candle_count * 2  # 2 hours per candle on 2H timeframe
                
                cache[cache_key]['candle_count'] = new_candle_count
                
                if hours_in_zone >= 20 and not reminder_sent:  # FIXED: Use candle-based timing
                    result['send_alert'] = True
                    result['alert_type'] = 'EXTENDED SQUEEZE'
                    cache[cache_key]['reminder_sent'] = True
                    
                    print(f"🔔 REMINDER: {symbol} - in zone for {hours_in_zone}h ({new_candle_count} candles)")
                else:
                    print(f"📍 STAY IN ZONE: {symbol} - {hours_in_zone}h ({new_candle_count} candles) - NO ALERT")
            else:
                # In zone but didn't enter from above - no alert
                print(f"📍 IN ZONE (no entry from above): {symbol} - NO ALERT")
        else:
            if was_in_zone:
                # Exit zone
                cache[cache_key] = {
                    'in_zone': False,
                    'entry_time': 0,
                    'reminder_sent': False,
                    'candle_count': 0
                }
                
                print(f"🚪 EXIT ZONE: {symbol}")
            else:
                print(f"🌐 OUTSIDE ZONE: {symbol}")
        
        # Save to git
        self.save_cache(cache)
        
        return result
    
    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main analysis function"""
        try:
            closes = ohlcv_data['close']
            current_bbw, highest_expansion, lowest_contraction, bbw_history = self.calculate_bbw(closes)
            
            if current_bbw <= 0:
                return {'send_alert': False, 'error': 'Invalid BBW calculation'}
            
            # Check for squeeze alert with git persistence - ENHANCED with BBW history
            alert_result = self.check_squeeze_alert(symbol, current_bbw, lowest_contraction, bbw_history)
            
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
