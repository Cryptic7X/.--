"""
SMA Indicator - 50/200 Crossovers + Proximity Detection
2H timeframe with 12H suppression logic
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

class SMAIndicator:
    def __init__(self, config: Dict):
        self.config = config
        self.cache_file = config.get('cache_file', 'cache/sma_alerts.json')
        self.alert_cache = self.load_cache()
        
    def load_cache(self) -> Dict:
        """Load SMA alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"⚠️ SMA cache load error: {e}")
            return {}
    
    def save_cache(self):
        """Save SMA alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.alert_cache, f, indent=2)
        except Exception as e:
            print(f"⚠️ SMA cache save error: {e}")

    # Replace the ranging detection in SMA indicator with this corrected version:
    
    def calculate_sma_data(self, ohlcv_data: Dict) -> pd.DataFrame:
        """
        Calculate SMA 50 and 200 with crossover detection - FIXED RANGING THRESHOLD
        """
        try:
            # Convert dict to DataFrame (SAME FIX AS CIPHERB)
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            
            # Convert timestamp and set index
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            if len(df) < 200:  # Need at least 200 periods for SMA200
                return pd.DataFrame()
    
            # SMA calculations
            short_length = self.config.get('short_length', 50)
            long_length = self.config.get('long_length', 200)
            
            sma50 = df['close'].rolling(window=short_length).mean()
            sma200 = df['close'].rolling(window=long_length).mean()
            
            # Create results DataFrame
            results = pd.DataFrame(index=df.index)
            results['close'] = df['close']
            results['sma50'] = sma50
            results['sma200'] = sma200
            
            # Crossover detection
            results['sma50_prev'] = sma50.shift(1)
            results['sma200_prev'] = sma200.shift(1)
            
            # Golden Cross: 50 crosses ABOVE 200
            results['golden_cross'] = (
                (results['sma50_prev'] <= results['sma200_prev']) & 
                (results['sma50'] > results['sma200'])
            )
            
            # Death Cross: 50 crosses BELOW 200
            results['death_cross'] = (
                (results['sma50_prev'] >= results['sma200_prev']) & 
                (results['sma50'] < results['sma200'])
            )
            
            # Market trend determination
            results['market_trend'] = 'neutral'
            results.loc[results['sma50'] > results['sma200'], 'market_trend'] = 'bullish'
            results.loc[results['sma50'] < results['sma200'], 'market_trend'] = 'bearish'
            
            # FIXED: Ranging market detection (much stricter threshold)
            sma_gap = abs(results['sma50'] - results['sma200']) / ((results['sma50'] + results['sma200']) / 2)
            ranging_threshold = 0.005  # CHANGED: 0.5% instead of 2% (much stricter)
            results['ranging_market'] = sma_gap <= ranging_threshold
            
            return results
            
        except Exception as e:
            print(f"❌ SMA calculation failed: {e}")
            return pd.DataFrame()


    def detect_crossover_signals(self, symbol: str, sma_data: pd.DataFrame) -> List[Dict]:
        """
        Detect crossover signals (NO suppression - always alert)
        """
        if sma_data.empty:
            return []
        
        alerts = []
        latest = sma_data.iloc[-1]
        current_time = datetime.utcnow()
        
        try:
            # Golden Cross
            if latest['golden_cross']:
                alert = {
                    'type': 'golden_cross',
                    'symbol': symbol,
                    'timestamp': current_time.isoformat(),
                    'sma50': round(latest['sma50'], 4),
                    'sma200': round(latest['sma200'], 4),
                    'price': round(latest['close'], 4),
                    'suppression': None  # No suppression for crossovers
                }
                alerts.append(alert)
                
                # Update market trend in cache
                self.update_market_trend(symbol, 'bullish', current_time)
                
            # Death Cross
            if latest['death_cross']:
                alert = {
                    'type': 'death_cross',
                    'symbol': symbol,
                    'timestamp': current_time.isoformat(),
                    'sma50': round(latest['sma50'], 4),
                    'sma200': round(latest['sma200'], 4),
                    'price': round(latest['close'], 4),
                    'suppression': None  # No suppression for crossovers
                }
                alerts.append(alert)
                
                # Update market trend in cache
                self.update_market_trend(symbol, 'bearish', current_time)
            
            return alerts
            
        except Exception as e:
            print(f"❌ SMA crossover detection failed for {symbol}: {e}")
            return []

    def detect_proximity_signals(self, symbol: str, sma_data: pd.DataFrame) -> List[Dict]:
        """
        Detect proximity signals with 12H suppression
        """
        if sma_data.empty:
            return []
        
        alerts = []
        latest = sma_data.iloc[-1]
        current_time = datetime.utcnow()
        proximity_threshold = self.config.get('proximity_threshold', 0.02)  # 2%
        suppression_hours = self.config.get('proximity_suppression_hours', 12)  # 12H
        
        try:
            # Skip if ranging market
            if latest['ranging_market']:
                # Check if we've already sent ranging alert
                if not self.has_ranging_alert_sent(symbol):
                    alert = {
                        'type': 'ranging_market',
                        'symbol': symbol,
                        'timestamp': current_time.isoformat(),
                        'sma50': round(latest['sma50'], 4),
                        'sma200': round(latest['sma200'], 4),
                        'price': round(latest['close'], 4),
                        'gap_percentage': round(abs(latest['sma50'] - latest['sma200']) / ((latest['sma50'] + latest['sma200']) / 2) * 100, 2)
                    }
                    alerts.append(alert)
                    self.set_ranging_alert_sent(symbol, current_time)
                return alerts
            
            # Clear ranging status if not ranging anymore
            self.clear_ranging_status(symbol)
            
            # Get current market trend
            market_trend = self.get_market_trend(symbol)
            price = latest['close']
            
            # SMA50 Proximity Check
            sma50_distance = abs(price - latest['sma50']) / latest['sma50']
            if sma50_distance <= proximity_threshold:
                if self.can_send_proximity_alert(symbol, 'sma50', suppression_hours):
                    if market_trend == 'bullish' and price >= latest['sma50'] * 0.98:  # Approaching from above
                        alert = {
                            'type': 'support',
                            'level': 'SMA50',
                            'symbol': symbol,
                            'timestamp': current_time.isoformat(),
                            'sma_value': round(latest['sma50'], 4),
                            'price': round(price, 4),
                            'distance_percent': round(sma50_distance * 100, 2)
                        }
                        alerts.append(alert)
                        self.update_proximity_alert(symbol, 'sma50', current_time)
                        
                    elif market_trend == 'bearish' and price <= latest['sma50'] * 1.02:  # Approaching from below
                        alert = {
                            'type': 'resistance',
                            'level': 'SMA50',
                            'symbol': symbol,
                            'timestamp': current_time.isoformat(),
                            'sma_value': round(latest['sma50'], 4),
                            'price': round(price, 4),
                            'distance_percent': round(sma50_distance * 100, 2)
                        }
                        alerts.append(alert)
                        self.update_proximity_alert(symbol, 'sma50', current_time)
            
            # SMA200 Proximity Check
            sma200_distance = abs(price - latest['sma200']) / latest['sma200']
            if sma200_distance <= proximity_threshold:
                if self.can_send_proximity_alert(symbol, 'sma200', suppression_hours):
                    if market_trend == 'bullish' and price >= latest['sma200'] * 0.98:  # Approaching from above
                        alert = {
                            'type': 'support',
                            'level': 'SMA200',
                            'symbol': symbol,
                            'timestamp': current_time.isoformat(),
                            'sma_value': round(latest['sma200'], 4),
                            'price': round(price, 4),
                            'distance_percent': round(sma200_distance * 100, 2)
                        }
                        alerts.append(alert)
                        self.update_proximity_alert(symbol, 'sma200', current_time)
                        
                    elif market_trend == 'bearish' and price <= latest['sma200'] * 1.02:  # Approaching from below
                        alert = {
                            'type': 'resistance',
                            'level': 'SMA200',
                            'symbol': symbol,
                            'timestamp': current_time.isoformat(),
                            'sma_value': round(latest['sma200'], 4),
                            'price': round(price, 4),
                            'distance_percent': round(sma200_distance * 100, 2)
                        }
                        alerts.append(alert)
                        self.update_proximity_alert(symbol, 'sma200', current_time)
            
            return alerts
            
        except Exception as e:
            print(f"❌ SMA proximity detection failed for {symbol}: {e}")
            return []

    def update_market_trend(self, symbol: str, trend: str, timestamp: datetime):
        """Update market trend after crossover"""
        if symbol not in self.alert_cache:
            self.alert_cache[symbol] = {}
        
        self.alert_cache[symbol]['market_trend'] = trend
        self.alert_cache[symbol]['trend_updated'] = timestamp.isoformat()
        self.save_cache()

    def get_market_trend(self, symbol: str) -> str:
        """Get current market trend for symbol"""
        return self.alert_cache.get(symbol, {}).get('market_trend', 'neutral')

    def can_send_proximity_alert(self, symbol: str, level: str, suppression_hours: int) -> bool:
        """Check if proximity alert can be sent (12H suppression)"""
        cache_key = f"{symbol}_{level}_proximity"
        last_alert = self.alert_cache.get(cache_key, {})
        
        if not last_alert:
            return True
        
        last_time = datetime.fromisoformat(last_alert.get('timestamp', '2020-01-01'))
        hours_since = (datetime.utcnow() - last_time).total_seconds() / 3600
        
        return hours_since >= suppression_hours

    def update_proximity_alert(self, symbol: str, level: str, timestamp: datetime):
        """Update proximity alert timestamp"""
        cache_key = f"{symbol}_{level}_proximity"
        self.alert_cache[cache_key] = {'timestamp': timestamp.isoformat()}
        self.save_cache()

    def has_ranging_alert_sent(self, symbol: str) -> bool:
        """Check if ranging alert already sent"""
        return self.alert_cache.get(f"{symbol}_ranging", {}).get('sent', False)

    def set_ranging_alert_sent(self, symbol: str, timestamp: datetime):
        """Mark ranging alert as sent"""
        self.alert_cache[f"{symbol}_ranging"] = {
            'sent': True,
            'timestamp': timestamp.isoformat()
        }
        self.save_cache()

    def clear_ranging_status(self, symbol: str):
        """Clear ranging status when market exits range"""
        ranging_key = f"{symbol}_ranging"
        if ranging_key in self.alert_cache:
            del self.alert_cache[ranging_key]
            self.save_cache()

    def get_sma_context(self, ohlcv_data: Dict) -> Dict:
        """
        Get SMA context for display in other system alerts
        """
        try:
            sma_data = self.calculate_sma_data(ohlcv_data)
            
            if sma_data.empty:
                return {'sma50': 0, 'sma200': 0, 'trend': 'no_data'}
            
            latest = sma_data.iloc[-1]
            
            return {
                'sma50': round(latest['sma50'], 2),
                'sma200': round(latest['sma200'], 2),
                'trend': latest['market_trend'],
                'ranging': latest['ranging_market']
            }
            
        except Exception as e:
            return {'sma50': 0, 'sma200': 0, 'trend': 'error'}

def detect_sma_signals(ohlcv_data: Dict, config: Dict, symbol: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Main function to detect SMA crossover and proximity signals
    Returns (crossover_alerts, proximity_alerts)
    """
    sma_indicator = SMAIndicator(config)
    
    # Calculate SMA data
    sma_data = sma_indicator.calculate_sma_data(ohlcv_data)
    
    if sma_data.empty:
        return [], []
    
    # Detect crossover signals (no suppression)
    crossover_alerts = sma_indicator.detect_crossover_signals(symbol, sma_data)
    
    # Detect proximity signals (12H suppression)
    proximity_alerts = sma_indicator.detect_proximity_signals(symbol, sma_data)
    
    return crossover_alerts, proximity_alerts
