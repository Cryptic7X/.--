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
        self.ema_short = 21  
        self.ema_long = 50   
        self.cache_file = "cache/ema_crossover_alerts.json"
        self.crossover_cooldown_hours = 24

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
        """UPDATED: 21 EMA crossover with 50 EMA"""
        if len(ema12) < 2 or len(ema21) < 2:
            return None
        
        # Get previous and current values
        prev_12, curr_12 = ema12[-2], ema12[-1]
        prev_21, curr_21 = ema21[-2], ema21[-1]
        
        # Golden Cross: 21 EMA crosses above 50 EMA
        if prev_21 <= prev_50 and curr_21 > curr_50:
            return 'golden_cross'
        
        # Death Cross: 21 EMA crosses below 50 EMA
        if prev_21 >= prev_50 and curr_21 < curr_50:
            return 'death_cross'
        
        return None


    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
            try:
                closes = ohlcv_data['close']
                
                # CHANGED: Require more data for 1H accuracy
                if len(closes) < 100:
                    return {'crossover_alert': False}
                
                ema21 = self.calculate_ema(closes, self.ema_short)
                ema50 = self.calculate_ema(closes, self.ema_long)
                
                current_time = time.time()
                cache = self.load_ema_cache()
                cache_updated = False
                
                # CROSSOVER CHECK - ONLY LOGIC NEEDED
                crossover_type = self.detect_crossover(ema21, ema50)
                crossover_alert = False
                
                if crossover_type:
                    # CHANGED: Cache key is just symbol (blocks all crossover types)
                    crossover_key = f"{symbol}"  # SIMPLIFIED: No "_crossover" suffix
                    
                    if crossover_key in cache:
                        last_time = cache[crossover_key].get('last_alert_time', 0)
                        hours_since = (current_time - last_time) / 3600
                        
                        # 24-hour cooldown check
                        if hours_since >= self.crossover_cooldown_hours:
                            crossover_alert = True
                            cache[crossover_key] = {'last_alert_time': current_time}
                            cache_updated = True
                    else:
                        # First crossover for this symbol
                        crossover_alert = True
                        cache[crossover_key] = {'last_alert_time': current_time}
                        cache_updated = True
                
                # REMOVED: All zone logic completely
                
                # Save cache if updated
                if cache_updated:
                    self.save_ema_cache(cache)
                
                # SIMPLIFIED: Only return crossover data
                return {
                    'crossover_alert': crossover_alert,
                    'crossover_type': crossover_type if crossover_alert else None,
                    'ema21': ema21[-1],
                    'ema50': ema50[-1],
                    'current_price': closes[-1]
                }
                
            except Exception as e:
                print(f"❌ EMA analysis error for {symbol}: {e}")
                return {'crossover_alert': False}
