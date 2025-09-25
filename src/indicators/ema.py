"""
EMA Indicator - 21/50 EMA Crossovers + Zone Touch
4H Timeframe with Git-Persistent Cache
"""
import json
import os
import time
import subprocess
import threading
from typing import Dict, List, Tuple

class EMAIndicator:
    def __init__(self):
        # EMA periods
        self.ema_short = 21
        self.ema_long = 50
        
        # Git-persistent cache
        self.cache_file = os.path.abspath("cache/ema_alerts.json")
        self._cache_lock = threading.Lock()
        self._cache = None
        
        # Cooldown periods
        self.crossover_cooldown_hours = 48  # 2 days

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        """Calculate EMA exactly like TradingView"""
        if len(data) < period:
            return [0] * len(data)
        
        ema_values = []
        multiplier = 2 / (period + 1)
        
        # First EMA value is SMA
        sma = sum(data[:period]) / period
        ema_values.extend([0] * (period - 1))
        ema_values.append(sma)
        
        # Calculate remaining EMA values
        for i in range(period, len(data)):
            ema = (data[i] * multiplier) + (ema_values[i-1] * (1 - multiplier))
            ema_values.append(ema)
        
        return ema_values

    def commit_cache_to_git(self):
        """Commit cache to git for persistence"""
        try:
            subprocess.run(['git', 'config', 'user.name', 'EMA Cache Bot'], check=False)
            subprocess.run(['git', 'config', 'user.email', 'ema@cache.bot'], check=False)
            subprocess.run(['git', 'add', self.cache_file], check=False)
            
            result = subprocess.run(['git', 'commit', '-m', 'Update EMA cache'], 
                                  capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                print(f"✅ EMA CACHE COMMITTED TO GIT")
                subprocess.run(['git', 'push'], capture_output=True, text=True, check=False)
            else:
                print(f"📝 No EMA cache changes")
                
        except Exception as e:
            print(f"❌ EMA git commit error: {e}")

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
                            print(f"📁 LOADED EMA CACHE: {len(self._cache)} entries")
                            return self._cache
            except Exception as e:
                print(f"❌ EMA cache load error: {e}")
            
            print(f"📁 CREATING NEW EMA CACHE")
            self._cache = {}
            return self._cache

    def save_cache(self, cache_data: Dict):
        """Save cache and commit to git"""
        with self._cache_lock:
            self._cache = cache_data
            
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                
                with open(self.cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                print(f"💾 SAVED EMA CACHE: {len(cache_data)} entries")
                self.commit_cache_to_git()
                
            except Exception as e:
                print(f"❌ EMA cache save error: {e}")

    def detect_crossover(self, ema21: List[float], ema50: List[float]) -> str:
        """Detect crossover type"""
        if len(ema21) < 2 or len(ema50) < 2:
            return None
        
        prev_21, curr_21 = ema21[-2], ema21[-1]
        prev_50, curr_50 = ema50[-2], ema50[-1]
        
        # Golden Cross: 21 EMA crosses above 50 EMA
        if prev_21 <= prev_50 and curr_21 > curr_50:
            return 'golden_cross'
        
        # Death Cross: 21 EMA crosses below 50 EMA
        if prev_21 >= prev_50 and curr_21 < curr_50:
            return 'death_cross'
        
        return None

    def check_zone_touch(self, closes: List[float], ema21: List[float], ema50: List[float]) -> bool:
        """Check if price touches 21 EMA (zone entry)"""
        if len(closes) < 2 or len(ema21) < 2:
            return False
        
        current_price = closes[-1]
        current_ema21 = ema21[-1]
        
        # Price touches 21 EMA (within 0.5% tolerance)
        distance_percent = abs(current_price - current_ema21) / current_ema21 * 100
        return distance_percent <= 0.5

    def check_crossover_alert(self, symbol: str, crossover_type: str) -> Dict:
        """Check crossover with 2-day cooldown"""
        current_time = time.time()
        cache = self.load_cache()
        crossover_key = f"{symbol}_crossover"
        
        result = {
            'send_alert': False,
            'alert_type': None
        }
        
        if not crossover_type:
            return result
        
        # Check cooldown
        if crossover_key in cache:
            last_crossover_time = cache[crossover_key].get('last_alert_time', 0)
            hours_since_last = (current_time - last_crossover_time) / 3600
            
            if hours_since_last < self.crossover_cooldown_hours:
                print(f"🕒 CROSSOVER COOLDOWN: {symbol} - {hours_since_last:.1f}h ago")
                return result
        
        # Send crossover alert
        result['send_alert'] = True
        result['alert_type'] = crossover_type
        
        cache[crossover_key] = {
            'last_alert_time': current_time,
            'crossover_type': crossover_type
        }
        
        print(f"📈 CROSSOVER ALERT: {symbol} - {crossover_type.upper()}")
        self.save_cache(cache)
        return result

    def check_zone_touch_alert(self, symbol: str, is_zone_touch: bool) -> Dict:
        """Check zone touch with deduplication (like BBW)"""
        current_time = time.time()
        cache = self.load_cache()
        zone_key = f"{symbol}_zone"
        
        result = {
            'send_alert': False,
            'alert_type': None
        }
        
        if not is_zone_touch:
            # Not in zone - reset cache
            if zone_key in cache:
                cache[zone_key] = {'in_zone': False}
                self.save_cache(cache)
            return result
        
        # Check if already alerted for this zone entry
        if zone_key in cache:
            was_in_zone = cache[zone_key].get('in_zone', False)
            if was_in_zone:
                print(f"📍 STAY IN ZONE: {symbol} - NO ALERT")
                return result
        
        # First zone touch - send alert
        result['send_alert'] = True
        result['alert_type'] = 'zone_touch'
        
        cache[zone_key] = {
            'in_zone': True,
            'entry_time': current_time
        }
        
        print(f"🎯 ZONE TOUCH: {symbol} - 21 EMA Touch")
        self.save_cache(cache)
        return result

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Main EMA analysis - Crossover first, then Zone"""
        try:
            closes = ohlcv_data['close']
            
            if len(closes) < max(self.ema_short, self.ema_long) + 2:
                return {'crossover_alert': False, 'zone_alert': False}
            
            # Calculate EMAs
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)
            
            # 1. PRIORITY: Check crossover first
            crossover_type = self.detect_crossover(ema21, ema50)
            crossover_result = self.check_crossover_alert(symbol, crossover_type)
            
            # 2. Check zone touch (only after crossover logic)
            is_zone_touch = self.check_zone_touch(closes, ema21, ema50)
            zone_result = self.check_zone_touch_alert(symbol, is_zone_touch)
            
            return {
                'crossover_alert': crossover_result['send_alert'],
                'crossover_type': crossover_result['alert_type'],
                'zone_alert': zone_result['send_alert'],
                'zone_type': zone_result['alert_type'],
                'ema21': ema21[-1],
                'ema50': ema50[-1],
                'current_price': closes[-1]
            }
            
        except Exception as e:
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False, 'zone_alert': False, 'error': str(e)}
