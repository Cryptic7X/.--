"""
BBW Indicator - STRICTER SQUEEZE DETECTION
Only alert when BBW is REALLY in squeeze zone
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
        """Load alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_cache(self, cache_data: Dict):
        """Save alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f)
        except:
            pass

    def is_squeeze_entry(self, symbol: str, current_bbw: float, lowest_contraction: float) -> bool:
        """
        STRICTER squeeze detection to reduce false positives:
        - Calculate 50% above lowest contraction (not 75%)
        - Alert when BBW first enters range [lowest_contraction to 50% above]
        - Only alert if current BBW is VERY close to contraction line
        """
        # STRICTER: Only 50% above contraction (was 75%)
        squeeze_threshold = lowest_contraction * 1.50  # 50% above instead of 75%
        
        # STRICTER: BBW must be very close to contraction line (within 25% above)
        close_to_contraction_threshold = lowest_contraction * 1.25  # 25% above contraction
        
        # Check if BBW is in the tight squeeze range
        is_very_close_to_contraction = lowest_contraction <= current_bbw <= close_to_contraction_threshold
        
        if not is_very_close_to_contraction:
            return False
        
        # Deduplication - only alert once per 48 hours (was 24)
        cache = self.load_cache()
        current_time = time.time()
        
        if symbol in cache:
            last_alert = cache[symbol]
            if (current_time - last_alert) < (48 * 60 * 60):  # 48 hours
                return False
        
        # Save alert time
        cache[symbol] = current_time
        self.save_cache(cache)
        
        print(f"BBW STRICT squeeze: {symbol} BBW:{current_bbw:.2f} in tight range [{lowest_contraction:.2f} - {close_to_contraction_threshold:.2f}]")
        return True

    def calculate_bbw_signals(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main BBW calculation with STRICT logic"""
        try:
            closes = ohlcv_data['close']
            bbw_values, highest_expansion, lowest_contraction = self.calculate_bbw(closes)
            
            if not bbw_values:
                return {'squeeze_signal': False, 'error': 'Insufficient data'}
            
            current_bbw = bbw_values[-1]
            squeeze_threshold = lowest_contraction * 1.50  # 50% threshold for display
            
            # STRICT squeeze entry detection
            squeeze_detected = self.is_squeeze_entry(symbol, current_bbw, lowest_contraction)
            
            return {
                'squeeze_signal': squeeze_detected,
                'bbw': current_bbw,
                'lowest_contraction': lowest_contraction,
                'highest_expansion': highest_expansion,
                'squeeze_threshold': squeeze_threshold
            }
            
        except Exception as e:
            return {'squeeze_signal': False, 'error': str(e)}
