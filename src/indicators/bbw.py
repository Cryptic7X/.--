"""
BBW Indicator - 30M with 100% Range Logic
Alerts only when BBW first enters the defined 100% range
"""
import numpy as np
import json
import os
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

class BBWIndicator:
    def __init__(self, length: int = 20, mult: float = 2.0, expansion_length: int = 125, contraction_length: int = 125):
        self.length = length
        self.mult = mult
        self.expansion_length = expansion_length
        self.contraction_length = contraction_length
        self.cache_file = "cache/bbw_alerts.json"

    def calculate_sma(self, data: List[float], period: int) -> List[float]:
        """Calculate Simple Moving Average"""
        if len(data) < period:
            return [0.0] * len(data)
        
        sma = []
        for i in range(len(data)):
            if i < period - 1:
                sma.append(0.0)
            else:
                avg = sum(data[i - period + 1:i + 1]) / period
                sma.append(avg)
        return sma

    def calculate_stddev(self, data: List[float], period: int) -> List[float]:
        """Calculate Standard Deviation"""
        if len(data) < period:
            return [0.0] * len(data)
        
        sma = self.calculate_sma(data, period)
        stddev = []
        
        for i in range(len(data)):
            if i < period - 1:
                stddev.append(0.0)
            else:
                variance = sum((data[j] - sma[i]) ** 2 for j in range(i - period + 1, i + 1)) / period
                stddev.append(np.sqrt(variance))
        
        return stddev

    def calculate_bollinger_bands(self, closes: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """Calculate Bollinger Bands"""
        basis = self.calculate_sma(closes, self.length)
        dev = self.calculate_stddev(closes, self.length)
        
        upper = [basis[i] + (self.mult * dev[i]) for i in range(len(basis))]
        lower = [basis[i] - (self.mult * dev[i]) for i in range(len(basis))]
        
        return upper, lower, basis

    def calculate_bbw(self, closes: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """Calculate Bollinger BandWidth with dynamic thresholds"""
        upper, lower, basis = self.calculate_bollinger_bands(closes)
        
        # Calculate BBW
        bbw = []
        for i in range(len(upper)):
            if basis[i] != 0:
                bbw_val = ((upper[i] - lower[i]) / basis[i]) * 100
            else:
                bbw_val = 0
            bbw.append(bbw_val)
        
        # Calculate dynamic thresholds
        highest_expansion = self.calculate_rolling_max(bbw, self.expansion_length)
        lowest_contraction = self.calculate_rolling_min(bbw, self.contraction_length)
        
        return bbw, highest_expansion, lowest_contraction

    def calculate_rolling_max(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling maximum"""
        rolling_max = []
        for i in range(len(data)):
            if i < period - 1:
                rolling_max.append(max(data[:i + 1]) if data[:i + 1] else 0)
            else:
                rolling_max.append(max(data[i - period + 1:i + 1]))
        return rolling_max

    def calculate_rolling_min(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling minimum"""
        rolling_min = []
        for i in range(len(data)):
            if i < period - 1:
                rolling_min.append(min(data[:i + 1]) if data[:i + 1] else 0)
            else:
                rolling_min.append(min(data[i - period + 1:i + 1]))
        return rolling_min

    def load_bbw_cache(self) -> Dict:
        """Load BBW alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading BBW cache: {e}")
        return {}

    def save_bbw_cache(self, cache_data: Dict) -> bool:
        """Save BBW alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving BBW cache: {e}")
            return False

    def detect_100_percent_range_entry(self, symbol: str, bbw_values: List[float], highest_expansion: List[float], lowest_contraction: List[float]) -> bool:
        """
        NEW: Detect first entry into 100% range (from highest_expansion to lowest_contraction)
        
        100% Range Logic:
        - Alert only when BBW first enters the defined 100% range
        - Range is from lowest_contraction to highest_expansion  
        - Deduplication: Only alert again after BBW exits range and re-enters
        """
        if len(bbw_values) < 3 or len(highest_expansion) < 3 or len(lowest_contraction) < 3:
            return False

        current_bbw = bbw_values[-1]
        expansion_line = highest_expansion[-1]  # Top of 100% range
        contraction_line = lowest_contraction[-1]  # Bottom of 100% range
        
        # Check if BBW is within the 100% range
        is_in_range = contraction_line <= current_bbw <= expansion_line
        
        if not is_in_range:
            return False

        # Load cache for deduplication
        cache = self.load_bbw_cache()
        cache_key = f"{symbol}_100_range"
        current_time = time.time()

        # Check if we already alerted for this range entry
        if cache_key in cache:
            last_alert_time = cache[cache_key].get('last_alert_time', 0)
            was_in_range = cache[cache_key].get('was_in_range', False)
            
            # If we were already in range, don't alert again
            if was_in_range:
                # Update cache to track current state
                cache[cache_key]['was_in_range'] = True
                cache[cache_key]['last_bbw_value'] = current_bbw
                self.save_bbw_cache(cache)
                return False
            
            # Check if we've exited and now re-entered (need to look at recent history)
            if self.has_exited_and_reentered(bbw_values, expansion_line, contraction_line):
                print(f"BBW re-entry detected for {symbol}: {current_bbw:.4f} back in range [{contraction_line:.4f} - {expansion_line:.4f}]")
            else:
                return False
        else:
            # First time seeing this symbol
            print(f"BBW first-time 100% range entry for {symbol}: {current_bbw:.4f} in range [{contraction_line:.4f} - {expansion_line:.4f}]")

        # Update cache to mark we're now in range
        cache[cache_key] = {
            'last_alert_time': current_time,
            'last_bbw_value': current_bbw,
            'expansion_line': expansion_line,
            'contraction_line': contraction_line,
            'was_in_range': True
        }
        
        self.save_bbw_cache(cache)
        return True

    def has_exited_and_reentered(self, bbw_values: List[float], expansion_line: float, contraction_line: float, lookback_periods: int = 12) -> bool:
        """
        Check if BBW has exited the range and is now re-entering
        Look at last 12 periods (6 hours of 30M data)
        """
        if len(bbw_values) < lookback_periods:
            return True  # Allow if insufficient data
        
        # Look at recent history
        recent_bbw = bbw_values[-lookback_periods:]
        
        # Check if we were outside the range recently
        was_outside_recently = False
        for bbw_val in recent_bbw[:-1]:  # Exclude current value
            if bbw_val < contraction_line or bbw_val > expansion_line:
                was_outside_recently = True
                break
        
        return was_outside_recently

    def update_range_exit_status(self, symbol: str, bbw_values: List[float], highest_expansion: List[float], lowest_contraction: List[float]):
        """
        Update cache when BBW exits the 100% range
        This is called for every analysis to track range exits
        """
        if len(bbw_values) < 1:
            return

        current_bbw = bbw_values[-1]
        expansion_line = highest_expansion[-1] if highest_expansion else 0
        contraction_line = lowest_contraction[-1] if lowest_contraction else 0
        
        # Check if currently outside range
        is_outside_range = current_bbw < contraction_line or current_bbw > expansion_line
        
        if is_outside_range:
            cache = self.load_bbw_cache()
            cache_key = f"{symbol}_100_range"
            
            if cache_key in cache:
                # Mark that we're now outside the range
                cache[cache_key]['was_in_range'] = False
                cache[cache_key]['last_bbw_value'] = current_bbw
                self.save_bbw_cache(cache)

    def calculate_bbw_signals(self, ohlcv_data: Dict[str, List[float]], symbol: str) -> Dict:
        """
        Main BBW calculation and 100% range signal detection
        """
        try:
            closes = ohlcv_data['close']
            if len(closes) < max(self.length, self.expansion_length, self.contraction_length) + 10:
                return {
                    'error': 'Insufficient data for BBW calculation',
                    'squeeze_signal': False
                }

            # Calculate BBW and thresholds
            bbw, highest_expansion, lowest_contraction = self.calculate_bbw(closes)

            # Update range exit status first
            self.update_range_exit_status(symbol, bbw, highest_expansion, lowest_contraction)

            # Detect 100% range entry
            range_entry_signal = self.detect_100_percent_range_entry(symbol, bbw, highest_expansion, lowest_contraction)

            current_bbw = bbw[-1]
            expansion_line = highest_expansion[-1]
            contraction_line = lowest_contraction[-1]

            return {
                'squeeze_signal': range_entry_signal,  # True when first entering 100% range
                'bbw': current_bbw,
                'lowest_contraction': contraction_line,
                'highest_expansion': expansion_line,
                'range_percentage': ((current_bbw - contraction_line) / (expansion_line - contraction_line)) * 100 if expansion_line > contraction_line else 0,
                'is_in_range': contraction_line <= current_bbw <= expansion_line
            }

        except Exception as e:
            print(f"Error calculating BBW signals for {symbol}: {e}")
            return {
                'error': str(e),
                'squeeze_signal': False
            }

# Global instance
bbw_indicator = BBWIndicator()
