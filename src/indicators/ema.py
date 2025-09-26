"""
EMA Indicator - BULLETPROOF BLOCKING SYSTEM
Using BBW-style cache logic for perfect deduplication
"""
import json
import os
import time
import subprocess
import threading
from typing import Dict, List

class EMAIndicator:
    def __init__(self):
        self.ema_short = 21
        self.ema_long = 50
        self.cache_file = os.path.abspath("cache/ema_alerts.json")
        self._cache_lock = threading.Lock()
        self.crossover_cooldown_hours = 48

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        """Calculate EMA like TradingView"""
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

    def load_cache(self) -> Dict:
        """Load cache (BBW-style)"""
        with self._cache_lock:
            try:
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        return json.load(f)
            except:
                pass
            return {}

    def save_cache(self, cache_data: Dict):
        """Save cache with git commit (BBW-style)"""
        with self._cache_lock:
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                with open(self.cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                # Git commit (same as BBW)
                subprocess.run(['git', 'config', 'user.name', 'EMA Bot'], check=False)
                subprocess.run(['git', 'config', 'user.email', 'ema@bot.com'], check=False)
                subprocess.run(['git', 'add', self.cache_file], check=False)
                result = subprocess.run(['git', 'commit', '-m', 'Update EMA cache'], 
                                      capture_output=True, text=True, check=False)
                if result.returncode == 0:
                    subprocess.run(['git', 'push'], capture_output=True, text=True, check=False)
            except:
                pass

    def detect_crossover(self, ema21: List[float], ema50: List[float]) -> str:
        """Detect crossover"""
        if len(ema21) < 2 or len(ema50) < 2:
            return None
        
        prev_21, curr_21 = ema21[-2], ema21[-1]
        prev_50, curr_50 = ema50[-2], ema50[-1]
        
        if prev_21 <= prev_50 and curr_21 > curr_50:
            return 'golden_cross'
        if prev_21 >= prev_50 and curr_21 < curr_50:
            return 'death_cross'
        
        return None

    def check_zone_touch(self, closes: List[float], ema21: List[float]) -> bool:
        """Check if price touches 21 EMA"""
        if len(closes) < 1 or len(ema21) < 1:
            return False
        
        current_price = closes[-1]
        current_ema21 = ema21[-1]
        
        # Price touches 21 EMA (within 0.5% tolerance)
        distance_percent = abs(current_price - current_ema21) / current_ema21 * 100
        return distance_percent <= 0.5

    def has_moved_away_from_zone(self, closes: List[float], ema21_values: List[float], last_alert_price: float) -> bool:
        """BBW-style movement detection: Has price moved significantly away from 21 EMA since last alert?"""
        if len(closes) < 12 or len(ema21_values) < 12:  # Need at least 12 periods (48H on 4H chart)
            return True  # Allow if insufficient data
        
        # Look at last 12 periods (48 hours on 4H)
        recent_closes = closes[-12:]
        recent_ema21 = ema21_values[-12:]
        
        # Calculate maximum distance from 21 EMA in recent periods
        max_distance_percent = 0
        for i in range(len(recent_closes)):
            if recent_ema21[i] > 0:
                distance = abs(recent_closes[i] - recent_ema21[i]) / recent_ema21[i] * 100
                max_distance_percent = max(max_distance_percent, distance)
        
        # Must have moved away at least 2% from 21 EMA since last alert
        return max_distance_percent > 2.0

    def detect_first_touch_zone(self, symbol: str, closes: List[float], ema21_values: List[float]) -> bool:
        """BBW-style zone touch detection with bulletproof blocking"""
        # Check if currently touching zone
        if not self.check_zone_touch(closes, ema21_values):
            return False
        
        # Load cache
        cache = self.load_cache()
        cache_key = f"{symbol}_zone_touch"
        current_time = time.time()
        current_price = closes[-1]
        
        # Check if we've alerted before
        if cache_key in cache:
            last_alert_time = cache[cache_key].get('last_alert_time', 0)
            last_alert_price = cache[cache_key].get('last_alert_price', 0)
            
            # Check if we've moved away significantly (BBW-style)
            if not self.has_moved_away_from_zone(closes, ema21_values, last_alert_price):
                # Still close to zone since last alert - block duplicate
                return False
        
        # This is a new valid zone touch - update cache
        cache[cache_key] = {
            'last_alert_time': current_time,
            'last_alert_price': current_price,
            'last_ema21': ema21_values[-1]
        }
        
        self.save_cache(cache)
        return True

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main EMA analysis with BULLETPROOF blocking"""
        try:
            closes = ohlcv_data['close']
            if len(closes) < max(self.ema_short, self.ema_long) + 2:
                return {'crossover_alert': False, 'zone_alert': False}
            
            # Calculate EMAs
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)
            
            current_time = time.time()
            cache = self.load_cache()
            
            # 1. CROSSOVER LOGIC (48H cooldown)
            crossover_type = self.detect_crossover(ema21, ema50)
            crossover_alert = False
            
            if crossover_type:
                crossover_key = f"{symbol}_crossover"
                if crossover_key in cache:
                    last_time = cache[crossover_key].get('last_alert_time', 0)
                    hours_since = (current_time - last_time) / 3600
                    if hours_since >= self.crossover_cooldown_hours:
                        crossover_alert = True
                        cache[crossover_key] = {
                            'last_alert_time': current_time,
                            'crossover_type': crossover_type
                        }
                        self.save_cache(cache)
                else:
                    crossover_alert = True
                    cache[crossover_key] = {
                        'last_alert_time': current_time,
                        'crossover_type': crossover_type
                    }
                    self.save_cache(cache)
            
            # 2. ZONE TOUCH LOGIC (BBW-style bulletproof blocking)
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
            return {'crossover_alert': False, 'zone_alert': False, 'error': str(e)}
