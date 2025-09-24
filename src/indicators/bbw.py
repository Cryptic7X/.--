"""
BBW Indicator - OPTIMIZED FOR SPEED
Fast BBW calculation with minimal file operations
"""
import numpy as np
import json
import os
import time
from typing import Dict, List, Tuple, Optional

class BBWIndicator:
    def __init__(self, length: int = 20, mult: float = 2.0):
        self.length = length
        self.mult = mult
        self.cache_file = "cache/bbw_alerts.json"
        # Load cache once at startup
        self._cache = self._load_cache()

    def _load_cache(self) -> Dict:
        """Load cache once at startup"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def _save_cache_batch(self, updates: Dict) -> None:
        """Save cache in batch (called once at end)"""
        try:
            self._cache.update(updates)
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self._cache, f)
        except Exception as e:
            print(f"Cache save error: {e}")

    def calculate_bbw_fast(self, closes: List[float]) -> Tuple[float, float, float, bool]:
        """
        FAST BBW calculation using numpy - returns only current values
        Returns: (current_bbw, contraction, expansion, is_squeeze)
        """
        try:
            closes_array = np.array(closes[-200:])  # Use only last 200 candles
            
            if len(closes_array) < max(self.length, 125):
                return 0, 0, 0, False
            
            # Calculate SMA and std using numpy (much faster)
            sma = np.convolve(closes_array, np.ones(self.length)/self.length, mode='valid')
            
            # Calculate rolling std
            rolling_std = np.array([
                np.std(closes_array[i:i+self.length]) 
                for i in range(len(closes_array) - self.length + 1)
            ])
            
            # Bollinger Bands
            upper = sma + (self.mult * rolling_std)
            lower = sma - (self.mult * rolling_std)
            
            # BBW calculation
            bbw = ((upper - lower) / sma) * 100
            
            # Get current values only (no rolling calculations)
            current_bbw = bbw[-1]
            
            # Simple min/max over last 125 periods (much faster than rolling)
            lookback = min(125, len(bbw))
            recent_bbw = bbw[-lookback:]
            
            contraction_line = np.min(recent_bbw)
            expansion_line = np.max(recent_bbw)
            
            # Calculate 75% threshold
            range_size = expansion_line - contraction_line
            threshold_75 = contraction_line + (range_size * 0.75)
            
            # Check if in squeeze (simple comparison)
            is_squeeze = contraction_line <= current_bbw <= threshold_75
            
            return current_bbw, contraction_line, expansion_line, is_squeeze
            
        except Exception as e:
            return 0, 0, 0, False

    def check_squeeze_signal(self, symbol: str, bbw: float, contraction: float, expansion: float, is_squeeze: bool) -> bool:
        """
        FAST squeeze signal check - minimal cache operations
        """
        if not is_squeeze:
            return False
        
        cache_key = f"{symbol}_sq"
        current_time = time.time()
        
        # Check cache in memory (no file I/O)
        if cache_key in self._cache:
            last_alert = self._cache[cache_key].get('last_alert', 0)
            was_in_squeeze = self._cache[cache_key].get('in_squeeze', False)
            
            # Don't alert if already in squeeze recently (2 hours = 7200 seconds)
            if was_in_squeeze and (current_time - last_alert) < 7200:
                return False
        
        # Mark as alerted (will be saved in batch later)
        self._cache[cache_key] = {
            'last_alert': current_time,
            'in_squeeze': True,
            'bbw': bbw
        }
        
        return True

    def calculate_bbw_signals(self, ohlcv_data: Dict[str, List[float]], symbol: str) -> Dict:
        """
        FAST main BBW calculation
        """
        try:
            closes = ohlcv_data['close']
            if len(closes) < 50:
                return {'squeeze_signal': False}

            # Fast BBW calculation
            bbw, contraction, expansion, is_squeeze = self.calculate_bbw_fast(closes)
            
            # Fast squeeze signal check
            squeeze_signal = self.check_squeeze_signal(symbol, bbw, contraction, expansion, is_squeeze)

            return {
                'squeeze_signal': squeeze_signal,
                'bbw': bbw,
                'lowest_contraction': contraction,
                'highest_expansion': expansion,
                'threshold_75': contraction + ((expansion - contraction) * 0.75),
                'is_in_squeeze': is_squeeze
            }

        except Exception as e:
            return {'squeeze_signal': False}

    def save_cache_updates(self, cache_updates: Dict) -> None:
        """Save all cache updates at once"""
        if cache_updates:
            self._save_cache_batch(cache_updates)
