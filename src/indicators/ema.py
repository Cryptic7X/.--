"""
EMA Indicator - FIXED CACHE LOGIC
21/50 EMA Crossovers + Zone Touch with Perfect Deduplication
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
        self._cache = None
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
        """Load git-persistent cache"""
        with self._cache_lock:
            if self._cache is not None:
                return self._cache
                
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        content = f.read()
                        if content.strip():
                            self._cache = json.loads(content)
                            return self._cache
            except:
                pass
            
            self._cache = {}
            return self._cache

    def save_cache(self, cache_data: Dict):
        """Save and commit cache"""
        with self._cache_lock:
            self._cache = cache_data
            
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                with open(self.cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                # Git commit
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

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main EMA analysis with FIXED cache logic"""
        try:
            closes = ohlcv_data['close']
            if len(closes) < max(self.ema_short, self.ema_long) + 2:
                return {'crossover_alert': False, 'zone_alert': False}
            
            # Calculate EMAs
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)
            
            current_time = time.time()
            cache = self.load_cache()
            
            # 1. CROSSOVER LOGIC
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
                else:
                    crossover_alert = True
                    cache[crossover_key] = {
                        'last_alert_time': current_time,
                        'crossover_type': crossover_type
                    }
            
            # 2. ZONE TOUCH LOGIC (FIXED - Same as BBW)
            is_zone_touch = self.check_zone_touch(closes, ema21)
            zone_alert = False
            zone_key = f"{symbol}_zone"
            
            if is_zone_touch:
                # Price is touching zone
                if zone_key in cache:
                    already_in_zone = cache[zone_key].get('in_zone', False)
                    if not already_in_zone:
                        # First touch - send alert
                        zone_alert = True
                        cache[zone_key] = {'in_zone': True, 'entry_time': current_time}
                    # else: already alerted, no new alert
                else:
                    # First time seeing this coin - send alert
                    zone_alert = True
                    cache[zone_key] = {'in_zone': True, 'entry_time': current_time}
            else:
                # Price NOT touching zone
                if zone_key in cache:
                    cache[zone_key] = {'in_zone': False}
            
            # Save cache if any changes
            if crossover_alert or zone_alert:
                self.save_cache(cache)
            
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
