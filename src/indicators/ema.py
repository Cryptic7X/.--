"""
EMA Indicator - SIMPLE ZONE BLOCKING
Uses YOUR EXACT BBW cache logic - nothing more, nothing less
"""
import json
import os
import time
import subprocess
from typing import Dict, List

class EMAIndicator:
    def __init__(self):
        self.ema_short = 21
        self.ema_long = 50
        self.cache_file = "cache/ema_zone_alerts.json"  # NEW CACHE FILE
        self.crossover_cooldown_hours = 48

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        if len(data) < period:
            return [0] * len(data)
        
        ema_values = []
        multiplier = 2 / (period + 1)
        sma = sum(data[:period]) / period
        ema_values.extend([0] * (period - 1))
        ema_values.append(sma)
        
        for i in range(period, len(data)):
            ema = (data[i] * multiplier) + (ema_values[i-1] * (1 - multiplier))
            ema_values.append(ema)
        
        return ema_values

    def load_ema_cache(self) -> Dict:
        """Load EMA cache - EXACT BBW METHOD"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_ema_cache(self, cache_data: Dict) -> bool:
        """Save EMA cache - EXACT BBW METHOD"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Git commit
            subprocess.run(['git', 'add', self.cache_file], check=False)
            subprocess.run(['git', 'commit', '-m', 'Update EMA cache'], check=False)
            subprocess.run(['git', 'push'], check=False)
            return True
        except:
            return False

    def detect_first_touch_zone(self, symbol: str, closes: List[float], ema21: List[float]) -> bool:
        """Detect first touch of 21 EMA - YOUR EXACT BBW LOGIC"""
        if len(closes) < 3 or len(ema21) < 3:
            return False

        current_price = closes[-1]
        ema21_value = ema21[-1]

        # Is touching 21 EMA (within 0.5% tolerance)
        is_touching = abs(current_price - ema21_value) / ema21_value <= 0.005

        if not is_touching:
            return False

        # Check cache for deduplication
        cache = self.load_ema_cache()
        cache_key = f"{symbol}_zone"
        current_time = time.time()

        if cache_key in cache:
            last_alert_time = cache[cache_key].get('last_alert_time', 0)
            last_price_value = cache[cache_key].get('last_price_value', 0)

            # Primary deduplication: Check if moved away significantly
            if not self.has_moved_away_significantly(closes, last_price_value):
                return False

        # Update cache
        cache[cache_key] = {
            'last_alert_time': current_time,
            'last_price_value': current_price,
            'ema21_threshold': ema21_value
        }
        self.save_ema_cache(cache)
        return True

    def has_moved_away_significantly(self, closes: List[float], last_alert_price: float) -> bool:
        """Check if price moved away significantly - YOUR EXACT BBW LOGIC"""
        if len(closes) < 20:  # Need at least some history
            return True  # Allow if insufficient data

        # Look at recent max/min (last 20 periods = 80 hours on 4H)
        recent_max = max(closes[-20:])
        recent_min = min(closes[-20:])

        # Must have moved at least 5% away from last alert price
        if last_alert_price > 0:
            max_expansion = recent_max / last_alert_price if last_alert_price > 0 else 2.0
            min_expansion = last_alert_price / recent_min if recent_min > 0 else 2.0
            return max_expansion >= 1.05 or min_expansion >= 1.05

        return True

    def detect_crossover(self, ema21: List[float], ema50: List[float]) -> str:
        if len(ema21) < 2 or len(ema50) < 2:
            return None
        
        prev_21, curr_21 = ema21[-2], ema21[-1]
        prev_50, curr_50 = ema50[-2], ema50[-1]
        
        if prev_21 <= prev_50 and curr_21 > curr_50:
            return 'golden_cross'
        if prev_21 >= prev_50 and curr_21 < curr_50:
            return 'death_cross'
        return None

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main analysis - SIMPLE"""
        try:
            closes = ohlcv_data['close']
            if len(closes) < 60:  # Need enough data
                return {'crossover_alert': False, 'zone_alert': False}
            
            # Calculate EMAs
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)
            
            # 1. Crossover check (48h cooldown)
            crossover_type = self.detect_crossover(ema21, ema50)
            crossover_alert = False
            
            if crossover_type:
                cache = self.load_ema_cache()
                crossover_key = f"{symbol}_crossover"
                current_time = time.time()
                
                if crossover_key in cache:
                    last_time = cache[crossover_key].get('last_alert_time', 0)
                    hours_since = (current_time - last_time) / 3600
                    if hours_since >= self.crossover_cooldown_hours:
                        crossover_alert = True
                        cache[crossover_key] = {'last_alert_time': current_time}
                        self.save_ema_cache(cache)
                else:
                    crossover_alert = True
                    cache[crossover_key] = {'last_alert_time': current_time}
                    self.save_ema_cache(cache)
            
            # 2. Zone touch check - YOUR EXACT LOGIC
            zone_alert = self.detect_first_touch_zone(symbol, closes, ema21)
            
            return {
                'crossover_alert': crossover_alert,
                'crossover_type': crossover_type if crossover_alert else None,
                'zone_alert': zone_alert,
                'zone_type': 'zone_touch' if zone_alert else None,
                'ema21': ema21[-1],
                'ema50': ema50[-1],
                'current_price': closes[-1]
            }
            
        except Exception as e:
            return {'crossover_alert': False, 'zone_alert': False}
