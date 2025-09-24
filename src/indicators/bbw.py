"""
BBW Indicator - CORRECT 75% Range Logic (Matching Pine Script)
BBW Squeeze: When BBW enters range from contraction_line to 75% of full range
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
        """Calculate Simple Moving Average (matching ta.sma)"""
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
        """Calculate Standard Deviation (matching ta.stdev)"""
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

    def calculate_bbw(self, closes: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """
        Calculate BBW exactly matching Pine Script:
        basis = ta.sma(src, length)
        dev = mult * ta.stdev(src, length)  
        upper = basis + dev
        lower = basis - dev
        bbw = ((upper - lower) / basis) * 100
        """
        basis = self.calculate_sma(closes, self.length)
        dev = self.calculate_stddev(closes, self.length)
        
        upper = [basis[i] + (self.mult * dev[i]) for i in range(len(basis))]
        lower = [basis[i] - (self.mult * dev[i]) for i in range(len(basis))]
        
        # BBW calculation exactly matching Pine Script
        bbw = []
        for i in range(len(upper)):
            if basis[i] != 0:
                bbw_val = ((upper[i] - lower[i]) / basis[i]) * 100
            else:
                bbw_val = 0
            bbw.append(bbw_val)
        
        # Calculate dynamic thresholds (matching ta.highest and ta.lowest)
        highest_expansion = self.calculate_rolling_max(bbw, self.expansion_length)
        lowest_contraction = self.calculate_rolling_min(bbw, self.contraction_length)
        
        return bbw, highest_expansion, lowest_contraction

    def calculate_rolling_max(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling maximum (matching ta.highest)"""
        rolling_max = []
        for i in range(len(data)):
            if i < period - 1:
                rolling_max.append(max(data[:i + 1]) if data[:i + 1] else 0)
            else:
                rolling_max.append(max(data[i - period + 1:i + 1]))
        return rolling_max

    def calculate_rolling_min(self, data: List[float], period: int) -> List[float]:
        """Calculate rolling minimum (matching ta.lowest)"""
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

    def calculate_75_percent_threshold(self, contraction_line: float, expansion_line: float) -> float:
        """
        CORRECTED: Calculate 75% threshold within the dynamic range
        
        Range = expansion_line - contraction_line  
        75% threshold = contraction_line + (range * 0.75)
        
        Example:
        - contraction_line = 2.23
        - expansion_line = 10.50  
        - range = 10.50 - 2.23 = 8.27
        - 75% threshold = 2.23 + (8.27 * 0.75) = 2.23 + 6.20 = 8.43
        """
        if expansion_line <= contraction_line:
            return contraction_line
        
        range_size = expansion_line - contraction_line
        threshold = contraction_line + (range_size * 0.75)
        return threshold

    def detect_squeeze_entry(self, symbol: str, bbw_values: List[float], lowest_contraction: List[float], highest_expansion: List[float]) -> bool:
        """
        CORRECTED: Detect BBW squeeze entry in dynamic 75% range
        
        Squeeze Range: From contraction_line to 75% of (expansion - contraction)
        Alert when BBW FIRST enters this range
        """
        if len(bbw_values) < 3 or len(lowest_contraction) < 3 or len(highest_expansion) < 3:
            return False

        current_bbw = bbw_values[-1]
        contraction_line = lowest_contraction[-1]
        expansion_line = highest_expansion[-1]
        
        # Calculate 75% threshold correctly
        threshold_75 = self.calculate_75_percent_threshold(contraction_line, expansion_line)
        
        # Check if BBW is in squeeze range (contraction to 75% threshold)
        is_in_squeeze_range = contraction_line <= current_bbw <= threshold_75
        
        if not is_in_squeeze_range:
            return False

        # Load cache for deduplication
        cache = self.load_bbw_cache()
        cache_key = f"{symbol}_squeeze"
        current_time = time.time()

        # Check if we already alerted for this squeeze
        if cache_key in cache:
            was_in_squeeze = cache[cache_key].get('was_in_squeeze', False)
            
            # If we were already in squeeze, don't alert again
            if was_in_squeeze:
                # Update cache to track current state
                cache[cache_key]['was_in_squeeze'] = True
                cache[cache_key]['last_bbw_value'] = current_bbw
                self.save_bbw_cache(cache)
                return False
            
            # Check if we've exited and now re-entered squeeze
            if self.has_exited_squeeze_and_reentered(bbw_values, contraction_line, threshold_75):
                print(f"BBW squeeze re-entry: {symbol} BBW:{current_bbw:.2f} in [{contraction_line:.2f} - {threshold_75:.2f}]")
            else:
                return False
        else:
            # First time seeing this symbol in squeeze
            print(f"BBW squeeze first entry: {symbol} BBW:{current_bbw:.2f} in [{contraction_line:.2f} - {threshold_75:.2f}]")

        # Update cache to mark we're now in squeeze
        cache[cache_key] = {
            'last_alert_time': current_time,
            'last_bbw_value': current_bbw,
            'contraction_line': contraction_line,
            'expansion_line': expansion_line,
            'threshold_75': threshold_75,
            'was_in_squeeze': True
        }
        
        self.save_bbw_cache(cache)
        return True

    def has_exited_squeeze_and_reentered(self, bbw_values: List[float], contraction_line: float, threshold_75: float, lookback_periods: int = 6) -> bool:
        """
        Check if BBW has exited squeeze range and is now re-entering
        Look at last 6 periods (12 hours of 2H data)
        """
        if len(bbw_values) < lookback_periods:
            return True  # Allow if insufficient data
        
        # Look at recent history
        recent_bbw = bbw_values[-lookback_periods:]
        
        # Check if we were outside squeeze range recently
        was_outside_recently = False
        for bbw_val in recent_bbw[:-1]:  # Exclude current value
            if bbw_val < contraction_line or bbw_val > threshold_75:
                was_outside_recently = True
                break
        
        return was_outside_recently

    def update_squeeze_exit_status(self, symbol: str, bbw_values: List[float], lowest_contraction: List[float], highest_expansion: List[float]):
        """
        Update cache when BBW exits squeeze range
        """
        if len(bbw_values) < 1:
            return

        current_bbw = bbw_values[-1]
        contraction_line = lowest_contraction[-1] if lowest_contraction else 0
        expansion_line = highest_expansion[-1] if highest_expansion else 0
        threshold_75 = self.calculate_75_percent_threshold(contraction_line, expansion_line)
        
        # Check if currently outside squeeze range
        is_outside_squeeze = current_bbw < contraction_line or current_bbw > threshold_75
        
        if is_outside_squeeze:
            cache = self.load_bbw_cache()
            cache_key = f"{symbol}_squeeze"
            
            if cache_key in cache:
                # Mark that we're now outside squeeze
                cache[cache_key]['was_in_squeeze'] = False
                cache[cache_key]['last_bbw_value'] = current_bbw
                self.save_bbw_cache(cache)

    def calculate_bbw_signals(self, ohlcv_data: Dict[str, List[float]], symbol: str) -> Dict:
        """
        Main BBW calculation and squeeze signal detection
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

            # Update squeeze exit status first
            self.update_squeeze_exit_status(symbol, bbw, lowest_contraction, highest_expansion)

            # Detect squeeze entry
            squeeze_entry_signal = self.detect_squeeze_entry(symbol, bbw, lowest_contraction, highest_expansion)

            current_bbw = bbw[-1]
            contraction_line = lowest_contraction[-1]
            expansion_line = highest_expansion[-1]
            threshold_75 = self.calculate_75_percent_threshold(contraction_line, expansion_line)

            return {
                'squeeze_signal': squeeze_entry_signal,  # True when first entering squeeze range
                'bbw': current_bbw,
                'lowest_contraction': contraction_line,
                'highest_expansion': expansion_line,
                'threshold_75': threshold_75,
                'is_in_squeeze': contraction_line <= current_bbw <= threshold_75
            }

        except Exception as e:
            print(f"Error calculating BBW signals for {symbol}: {e}")
            return {
                'error': str(e),
                'squeeze_signal': False
            }
