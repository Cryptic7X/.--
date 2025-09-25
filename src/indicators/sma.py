"""
Multi-Indicator Crypto Trading System - SMA Indicator
SMA crossovers and proximity detection with 12H suppression
NO RANGING ALERTS
"""
import json
import os
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SMAIndicator:
    def __init__(self, short_length: int = 50, long_length: int = 200):
        self.short_length = short_length
        self.long_length = long_length
        self.proximity_threshold = 0.02  # 2%
        self.suppression_hours = 12  # 12 hours
        self.cache_file = "cache/sma_alerts.json"

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

    def detect_crossovers(self, closes: List[float]) -> Dict:
        """Detect Golden Cross and Death Cross"""
        sma50 = self.calculate_sma(closes, self.short_length)
        sma200 = self.calculate_sma(closes, self.long_length)
        
        if len(sma50) < 2 or len(sma200) < 2:
            return {
                'golden_cross': False,
                'death_cross': False,
                'sma50': 0,
                'sma200': 0
            }
        
        # Current vs Previous values
        current_50 = sma50[-1]
        current_200 = sma200[-1]
        prev_50 = sma50[-2]
        prev_200 = sma200[-2]
        
        # Golden Cross: 50 crosses ABOVE 200
        golden_cross = (prev_50 <= prev_200) and (current_50 > current_200)
        
        # Death Cross: 50 crosses BELOW 200
        death_cross = (prev_50 >= prev_200) and (current_50 < current_200)
        
        return {
            'golden_cross': golden_cross,
            'death_cross': death_cross,
            'sma50': current_50,
            'sma200': current_200,
            'sma50_prev': prev_50,
            'sma200_prev': prev_200
        }

    def load_sma_cache(self) -> Dict:
        """Load SMA alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading SMA cache: {e}")
        return {}

    def save_sma_cache(self, cache_data: Dict) -> bool:
        """Save SMA alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving SMA cache: {e}")
            return False

    def check_proximity_suppression(self, symbol: str, alert_type: str) -> bool:
        """Check if proximity alert should be suppressed (12H rule)"""
        cache = self.load_sma_cache()
        cache_key = f"{symbol}_{alert_type}"
        
        if cache_key in cache:
            last_alert_time = cache[cache_key].get('last_alert_time', 0)
            current_time = time.time()
            
            # 12 hours = 43200 seconds
            if (current_time - last_alert_time) < 43200:
                return True  # Suppress
        
        return False  # Don't suppress

    def update_proximity_cache(self, symbol: str, alert_type: str):
        """Update proximity alert cache"""
        cache = self.load_sma_cache()
        cache_key = f"{symbol}_{alert_type}"
        
        cache[cache_key] = {
            'last_alert_time': time.time(),
            'alert_type': alert_type
        }
        
        self.save_sma_cache(cache)

    def get_market_trend(self, symbol: str) -> str:
        """Get current market trend from last crossover"""
        cache = self.load_sma_cache()
        trend_key = f"{symbol}_trend"
        return cache.get(trend_key, {}).get('trend', 'neutral')

    def set_market_trend(self, symbol: str, trend: str):
        """Set market trend after crossover"""
        cache = self.load_sma_cache()
        trend_key = f"{symbol}_trend"
        
        cache[trend_key] = {
            'trend': trend,  # 'bullish', 'bearish', 'neutral'
            'timestamp': time.time()
        }
        
        self.save_sma_cache(cache)

    def detect_proximity_alerts(self, symbol: str, price: float, sma50: float, sma200: float) -> List[Dict]:
        """Detect support/resistance proximity alerts"""
        alerts = []
        market_trend = self.get_market_trend(symbol)

        # SMA50 Proximity Check
        if sma50 > 0:
            sma50_distance = abs(price - sma50) / sma50
            if sma50_distance <= self.proximity_threshold:
                # Determine support/resistance based on trend
                if market_trend == 'bullish' and price >= sma50 * 0.98:  # Approaching from above
                    if not self.check_proximity_suppression(symbol, 'sma50_support'):
                        alerts.append({
                            'type': 'support',
                            'level': 'SMA50',
                            'sma_value': sma50,
                            'distance_percent': sma50_distance * 100
                        })
                        self.update_proximity_cache(symbol, 'sma50_support')
                        
                elif market_trend == 'bearish' and price <= sma50 * 1.02:  # Approaching from below
                    if not self.check_proximity_suppression(symbol, 'sma50_resistance'):
                        alerts.append({
                            'type': 'resistance',
                            'level': 'SMA50',
                            'sma_value': sma50,
                            'distance_percent': sma50_distance * 100
                        })
                        self.update_proximity_cache(symbol, 'sma50_resistance')

        # SMA200 Proximity Check (similar logic)
        if sma200 > 0:
            sma200_distance = abs(price - sma200) / sma200
            if sma200_distance <= self.proximity_threshold:
                if market_trend == 'bullish' and price >= sma200 * 0.98:
                    if not self.check_proximity_suppression(symbol, 'sma200_support'):
                        alerts.append({
                            'type': 'support',
                            'level': 'SMA200',
                            'sma_value': sma200,
                            'distance_percent': sma200_distance * 100
                        })
                        self.update_proximity_cache(symbol, 'sma200_support')
                        
                elif market_trend == 'bearish' and price <= sma200 * 1.02:
                    if not self.check_proximity_suppression(symbol, 'sma200_resistance'):
                        alerts.append({
                            'type': 'resistance',
                            'level': 'SMA200',
                            'sma_value': sma200,
                            'distance_percent': sma200_distance * 100
                        })
                        self.update_proximity_cache(symbol, 'sma200_resistance')

        return alerts

    def calculate_sma_signals(self, ohlcv_data: Dict[str, List[float]], symbol: str, price: float) -> Dict:
        """
        Main SMA calculation and signal detection
        NO RANGING ALERTS
        """
        try:
            closes = ohlcv_data['close']
            if len(closes) < max(self.short_length, self.long_length) + 5:
                return {
                    'error': 'Insufficient data for SMA calculation',
                    'crossover_alerts': [],
                    'proximity_alerts': []
                }

            # Detect crossovers
            crossover_data = self.detect_crossovers(closes)
            sma50 = crossover_data['sma50']
            sma200 = crossover_data['sma200']
            
            crossover_alerts = []
            proximity_alerts = []

            # Handle crossovers (NO SUPPRESSION)
            if crossover_data['golden_cross']:
                crossover_alerts.append({
                    'type': 'golden_cross',
                    'sma50': sma50,
                    'sma200': sma200
                })
                self.set_market_trend(symbol, 'bullish')
                
            elif crossover_data['death_cross']:
                crossover_alerts.append({
                    'type': 'death_cross',
                    'sma50': sma50,
                    'sma200': sma200
                })
                self.set_market_trend(symbol, 'bearish')

            # Check proximity alerts (NO RANGING CHECK)
            proximity_alerts = self.detect_proximity_alerts(symbol, price, sma50, sma200)

            return {
                'crossover_alerts': crossover_alerts,
                'proximity_alerts': proximity_alerts,
                'sma50': sma50,
                'sma200': sma200,
                'market_trend': self.get_market_trend(symbol)
            }

        except Exception as e:
            logger.error(f"Error calculating SMA signals for {symbol}: {e}")
            return {
                'error': str(e),
                'crossover_alerts': [],
                'proximity_alerts': []
            }

# Standalone function for backward compatibility  
def detect_sma_signals(ohlcv_data: Dict[str, List[float]], config: Dict, symbol: str) -> Tuple[List[Dict], List[Dict]]:
    """Detect SMA signals - backward compatibility function"""
    sma_indicator = SMAIndicator()
    
    # Get current price from closes
    current_price = ohlcv_data['close'][-1] if ohlcv_data['close'] else 0
    
    # Calculate signals
    result = sma_indicator.calculate_sma_signals(ohlcv_data, symbol, current_price)
    
    return result.get('crossover_alerts', []), result.get('proximity_alerts', [])
