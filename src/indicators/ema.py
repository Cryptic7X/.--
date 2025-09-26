"""
EMA Indicator - BULLETPROOF VERSION
No git dependency, simple file-based cache
"""
import json
import os
import time
from typing import Dict, List

class EMAIndicator:
    def __init__(self):
        self.ema_short = 21
        self.ema_long = 50
        self.cache_file = "cache/ema_zone_alerts.json"
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
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_ema_cache(self, cache_data: Dict):
        """Save cache - NO GIT REQUIRED"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

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

    def is_touching_zone(self, current_price: float, ema21_value: float) -> bool:
        """Zone touch check - 0.5% tolerance"""
        return abs(current_price - ema21_value) / ema21_value <= 0.005

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        try:
            closes = ohlcv_data['close']
            if len(closes) < 60:
                return {'crossover_alert': False, 'zone_alert': False}
            
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)
            
            current_time = time.time()
            cache = self.load_ema_cache()
            cache_updated = False
            
            # 1. CROSSOVER CHECK
            crossover_type = self.detect_crossover(ema21, ema50)
            crossover_alert = False
            
            if crossover_type:
                crossover_key = f"{symbol}_crossover"
                if crossover_key in cache:
                    last_time = cache[crossover_key].get('last_alert_time', 0)
                    hours_since = (current_time - last_time) / 3600
                    if hours_since >= self.crossover_cooldown_hours:
                        crossover_alert = True
                        cache[crossover_key] = {'last_alert_time': current_time}
                        cache_updated = True
                else:
                    crossover_alert = True
                    cache[crossover_key] = {'last_alert_time': current_time}
                    cache_updated = True
            
            # 2. ZONE TOUCH CHECK - VERY SIMPLE
            current_price = closes[-1]
            ema21_value = ema21[-1]
            is_touching = self.is_touching_zone(current_price, ema21_value)
            zone_alert = False
            
            zone_key = f"{symbol}_zone"
            
            if is_touching:
                if zone_key not in cache:
                    # First touch - alert
                    zone_alert = True
                    cache[zone_key] = {
                        'last_alert_time': current_time,
                        'last_price': current_price,
                        'blocked': True
                    }
                    cache_updated = True
                else:
                    # Already touched - check if should unblock
                    last_price = cache[zone_key].get('last_price', current_price)
                    blocked = cache[zone_key].get('blocked', False)
                    
                    if blocked:
                        # Check if moved away 3% to unblock
                        if abs(current_price - last_price) / last_price >= 0.03:
                            # Moved away enough - unblock for next touch
                            cache[zone_key]['blocked'] = False
                            cache[zone_key]['last_price'] = current_price
                            cache_updated = True
            else:
                # Not touching - unblock for future touches
                if zone_key in cache and cache[zone_key].get('blocked', False):
                    cache[zone_key]['blocked'] = False
                    cache_updated = True
            
            # Save cache if updated
            if cache_updated:
                self.save_ema_cache(cache)
            
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
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False, 'zone_alert': False}
