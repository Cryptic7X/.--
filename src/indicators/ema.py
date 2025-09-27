"""
EMA Indicator - 12/21 EMA CROSSOVER VERSION
Updated from 21/50 to 12/21 EMA crossover logic
"""

import json
import os
import time
from typing import Dict, List

class EMAIndicator:
    def __init__(self):
        self.ema_short = 12  # CHANGED: from 21 to 12
        self.ema_long = 21   # CHANGED: from 50 to 21
        self.cache_file = "cache/ema_zone_alerts.json"
        self.crossover_cooldown_hours = 48

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        if len(data) < period:
            return [0] * len(data)
        
        ema_values = []
        multiplier = 2 / (period + 1)
        
        # Start with SMA for the first EMA value
        sma = sum(data[:period]) / period
        ema_values.extend([0] * (period - 1))
        ema_values.append(sma)
        
        # Calculate EMA for the rest
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

    def detect_crossover(self, ema12: List[float], ema21: List[float]) -> str:
        """UPDATED: 12 EMA crossover with 21 EMA"""
        if len(ema12) < 2 or len(ema21) < 2:
            return None
        
        # Get previous and current values
        prev_12, curr_12 = ema12[-2], ema12[-1]
        prev_21, curr_21 = ema21[-2], ema21[-1]
        
        # Golden Cross: 12 EMA crosses above 21 EMA
        if prev_12 <= prev_21 and curr_12 > curr_21:
            return 'golden_cross'
        
        # Death Cross: 12 EMA crosses below 21 EMA
        if prev_12 >= prev_21 and curr_12 < curr_21:
            return 'death_cross'
        
        return None

    def is_touching_zone(self, current_price: float, ema12_value: float) -> bool:
        """Zone touch check - 0.5% tolerance - UPDATED: uses 12 EMA"""
        return abs(current_price - ema12_value) / ema12_value <= 0.005

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        try:
            closes = ohlcv_data['close']
            
            # Reduced minimum data requirement since 21 EMA needs less data than 50 EMA
            if len(closes) < 50:  # CHANGED: reduced from 60 to 50
                return {'crossover_alert': False, 'zone_alert': False}
            
            # Calculate 12 EMA and 21 EMA
            ema12 = self.calculate_ema(closes, self.ema_short)  # 12 EMA
            ema21 = self.calculate_ema(closes, self.ema_long)   # 21 EMA
            
            current_time = time.time()
            cache = self.load_ema_cache()
            cache_updated = False
            
            # 1. CROSSOVER CHECK - UPDATED: 12/21 crossover
            crossover_type = self.detect_crossover(ema12, ema21)
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
                    # First crossover for this symbol
                    crossover_alert = True
                    cache[crossover_key] = {'last_alert_time': current_time}
                    cache_updated = True
            
            # 2. ZONE TOUCH CHECK - UPDATED: uses 12 EMA for zone touch
            current_price = closes[-1]
            ema12_value = ema12[-1]
            is_touching = self.is_touching_zone(current_price, ema12_value)
            
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
                'ema12': ema12[-1],  # CHANGED: from ema21 to ema12
                'ema21': ema21[-1],  # CHANGED: from ema50 to ema21
                'current_price': closes[-1]
            }
            
        except Exception as e:
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False, 'zone_alert': False}