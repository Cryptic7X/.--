"""
Multi-Indicator Crypto Trading System - BBW Indicator
Bollinger BandWidth calculation with first-touch detection logic
"""

import numpy as np
import json
import os
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

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
            logger.error(f"Error loading BBW cache: {e}")
        
        return {}
    
    def save_bbw_cache(self, cache_data: Dict) -> bool:
        """Save BBW alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving BBW cache: {e}")
            return False
    
    def detect_first_touch_contraction(self, symbol: str, bbw_values: List[float], contraction_line: List[float]) -> bool:
        """
        Detect first touch of contraction line with deduplication logic
        """
        if len(bbw_values) < 3 or len(contraction_line) < 3:
            return False
        
        current_bbw = bbw_values[-1]
        contraction_threshold = contraction_line[-1]
        
        # Check if BBW is touching/below contraction line (1% tolerance)
        is_touching = current_bbw <= (contraction_threshold * 1.01)
        
        if not is_touching:
            return False
        
        # Load cache for deduplication
        cache = self.load_bbw_cache()
        cache_key = f"{symbol}_contraction"
        current_time = time.time()
        
        # Primary deduplication logic
        if cache_key in cache:
            last_alert_time = cache[cache_key].get('last_alert_time', 0)
            last_bbw_value = cache[cache_key].get('last_bbw_value', 0)
            
            # Check if we've moved away significantly (20% expansion required)
            if not self.has_moved_away_significantly(bbw_values, last_bbw_value):
                logger.info(f"BBW for {symbol} still in contraction, skipping duplicate alert")
                return False
        
        # Backup confirmation: check consecutive periods (for sharp movements)
        consecutive_confirmation = self.check_consecutive_contraction(bbw_values, contraction_line, periods=3)
        
        if consecutive_confirmation:
            # Update cache
            cache[cache_key] = {
                'last_alert_time': current_time,
                'last_bbw_value': current_bbw,
                'contraction_threshold': contraction_threshold
            }
            self.save_bbw_cache(cache)
            
            logger.info(f"BBW first-touch detected for {symbol}: {current_bbw:.4f} <= {contraction_threshold:.4f}")
            return True
        
        return False
    
    def has_moved_away_significantly(self, bbw_values: List[float], last_alert_bbw: float) -> bool:
        """Check if BBW has moved away significantly (20%+ expansion) since last alert"""
        if len(bbw_values) < 48:  # Need at least 24 hours of 30M data
            return True  # Allow if insufficient data
        
        # Look at last 48 periods (24 hours)
        recent_max = max(bbw_values[-48:])
        
        # Must have expanded 20% above the last alert BBW value
        expansion_ratio = recent_max / last_alert_bbw if last_alert_bbw > 0 else 2.0
        
        return expansion_ratio > 1.20
    
    def check_consecutive_contraction(self, bbw_values: List[float], contraction_line: List[float], periods: int = 3) -> bool:
        """Backup verification: check consecutive periods below contraction"""
        if len(bbw_values) < periods or len(contraction_line) < periods:
            return True  # Allow if insufficient data
        
        # Check last N periods
        for i in range(periods):
            bbw_val = bbw_values[-(i + 1)]
            contraction_val = contraction_line[-(i + 1)]
            
            if bbw_val > (contraction_val * 1.05):  # 5% tolerance for consecutive check
                return False
        
        return True
    
    def calculate_bbw_signals(self, ohlcv_data: Dict[str, List[float]], symbol: str) -> Dict:
        """
        Main BBW calculation and signal detection
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
            
            # Detect first-touch contraction
            squeeze_signal = self.detect_first_touch_contraction(symbol, bbw, lowest_contraction)
            
            return {
                'squeeze_signal': squeeze_signal,
                'bbw': bbw[-1],
                'lowest_contraction': lowest_contraction[-1],
                'highest_expansion': highest_expansion[-1],
                'contraction_ratio': bbw[-1] / lowest_contraction[-1] if lowest_contraction[-1] > 0 else 0,
                'is_contracted': bbw[-1] <= (lowest_contraction[-1] * 1.01)
            }
            
        except Exception as e:
            logger.error(f"Error calculating BBW signals for {symbol}: {e}")
            return {
                'error': str(e),
                'squeeze_signal': False
            }

# Global instance
bbw_indicator = BBWIndicator()
