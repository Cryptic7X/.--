"""
BBW (Bollinger Band Width) Indicator - 30M First-Touch Detection
Exact Pine Script implementation with deduplication logic
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

class BBWIndicator:
    def __init__(self, config: Dict):
        self.config = config
        self.cache_file = config.get('cache_file', 'cache/bbw_alerts.json')
        self.first_touch_cache = self.load_cache()
        
    def load_cache(self) -> Dict:
        """Load BBW first-touch cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"⚠️ BBW cache load error: {e}")
            return {}
    
    def save_cache(self):
        """Save BBW first-touch cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.first_touch_cache, f, indent=2)
        except Exception as e:
            print(f"⚠️ BBW cache save error: {e}")

    def calculate_bbw(self, ohlcv_data: Dict) -> pd.DataFrame:
        """
        Calculate BBW using exact Pine Script parameters
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(ohlcv_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            if len(df) < 125:  # Need enough data for lookback
                return pd.DataFrame()

            # Pine Script default parameters
            length = self.config.get('bollinger_length', 20)
            mult = self.config.get('bollinger_mult', 2.0)
            expansion_length = self.config.get('expansion_length', 125)
            contraction_length = self.config.get('contraction_length', 125)
            
            # Source (default close)
            src = df[self.config.get('bollinger_source', 'close')]
            
            # Bollinger Bands calculation
            basis = src.rolling(window=length).mean()  # ta.sma(src, length)
            std_dev = src.rolling(window=length).std() * mult  # mult * ta.stdev(src, length)
            upper = basis + std_dev
            lower = basis - std_dev
            
            # BBW calculation - exact Pine Script formula
            bbw = ((upper - lower) / basis) * 100  # ((upper - lower) / basis) * 100
            
            # Highest Expansion and Lowest Contraction
            highest_expansion = bbw.rolling(window=expansion_length).max()  # ta.highest(bbw, expansionLengthInput)
            lowest_contraction = bbw.rolling(window=contraction_length).min()  # ta.lowest(bbw, contractionLengthInput)
            
            # Create results DataFrame
            results = pd.DataFrame(index=df.index)
            results['bbw'] = bbw
            results['highest_expansion'] = highest_expansion
            results['lowest_contraction'] = lowest_contraction
            results['basis'] = basis
            results['upper'] = upper
            results['lower'] = lower
            
            return results
            
        except Exception as e:
            print(f"❌ BBW calculation failed: {e}")
            return pd.DataFrame()

    def detect_first_touch_contraction(self, symbol: str, bbw_data: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Detect first touch of lowest contraction line with deduplication
        """
        if bbw_data.empty:
            return False, {}
        
        try:
            current_time = datetime.utcnow()
            latest = bbw_data.iloc[-1]
            
            current_bbw = latest['bbw']
            contraction_line = latest['lowest_contraction']
            
            # Check if BBW is touching/below contraction line (1% tolerance)
            is_touching = current_bbw <= (contraction_line * 1.01)
            
            if not is_touching:
                return False, {}
            
            # Primary method: Check deduplication cache
            cache_key = f"{symbol}_contraction"
            last_alert = self.first_touch_cache.get(cache_key, {})
            
            if last_alert:
                last_alert_time = datetime.fromisoformat(last_alert.get('timestamp', '2020-01-01'))
                time_since_last = (current_time - last_alert_time).total_seconds() / 3600  # hours
                
                # Check if BBW has moved away significantly since last alert
                if time_since_last < 4:  # Within 4 hours
                    if not self.has_moved_away_significantly(symbol, bbw_data):
                        return False, {}  # Still in same squeeze
            
            # Backup method: Check consecutive periods (for sharp movements)
            consecutive_check = self.check_consecutive_contraction(bbw_data, contraction_line)
            
            if is_touching and (not last_alert or consecutive_check):
                # Valid first-touch detected
                alert_data = {
                    'timestamp': current_time.isoformat(),
                    'bbw_value': round(current_bbw, 4),
                    'contraction_line': round(contraction_line, 4),
                    'consecutive_confirmed': consecutive_check
                }
                
                # Update cache
                self.first_touch_cache[cache_key] = alert_data
                self.save_cache()
                
                return True, alert_data
            
            return False, {}
            
        except Exception as e:
            print(f"❌ BBW first-touch detection failed for {symbol}: {e}")
            return False, {}

    def has_moved_away_significantly(self, symbol: str, bbw_data: pd.DataFrame) -> bool:
        """
        Check if BBW has moved away 20%+ from contraction before new touch
        """
        try:
            # Look at recent BBW values (last 48 periods = 24 hours of 30M data)
            recent_data = bbw_data.tail(48)
            if len(recent_data) < 10:
                return True  # Not enough data, allow signal
            
            current_bbw = recent_data['bbw'].iloc[-1]
            recent_max = recent_data['bbw'].max()
            
            # Movement reset threshold (20% expansion)
            reset_threshold = self.config.get('movement_reset_threshold', 1.20)
            
            expansion_ratio = recent_max / current_bbw if current_bbw > 0 else 1.0
            
            return expansion_ratio >= reset_threshold
            
        except Exception as e:
            print(f"⚠️ BBW movement check failed: {e}")
            return True  # Allow signal if check fails

    def check_consecutive_contraction(self, bbw_data: pd.DataFrame, contraction_line: float) -> bool:
        """
        Backup: Check 2-3 consecutive periods below contraction (for sharp movements)
        """
        try:
            periods_to_check = self.config.get('consecutive_periods_check', 3)
            recent_bbw = bbw_data['bbw'].tail(periods_to_check)
            
            # Check if at least 2 out of last 3 periods are at/below contraction
            touching_count = sum(1 for bbw_val in recent_bbw if bbw_val <= contraction_line * 1.01)
            
            return touching_count >= 2
            
        except Exception as e:
            print(f"⚠️ BBW consecutive check failed: {e}")
            return True  # Allow signal if check fails

    def get_bbw_context(self, ohlcv_data: Dict) -> Dict:
        """
        Get BBW context for display in other system alerts
        """
        try:
            bbw_data = self.calculate_bbw(ohlcv_data)
            
            if bbw_data.empty:
                return {'bbw': 0, 'status': 'no_data'}
            
            latest = bbw_data.iloc[-1]
            current_bbw = latest['bbw']
            contraction_line = latest['lowest_contraction']
            
            # Determine BBW status
            if current_bbw <= contraction_line * 1.05:  # Within 5% of contraction
                status = 'squeeze'
            elif current_bbw >= latest['highest_expansion'] * 0.95:  # Near expansion
                status = 'expansion'
            else:
                status = 'normal'
            
            return {
                'bbw': round(current_bbw, 2),
                'contraction_line': round(contraction_line, 2),
                'status': status
            }
            
        except Exception as e:
            return {'bbw': 0, 'status': 'error'}

def detect_bbw_squeeze_signals(ohlcv_data: Dict, config: Dict) -> Tuple[bool, Dict]:
    """
    Main function to detect BBW squeeze signals with first-touch logic
    """
    bbw_indicator = BBWIndicator(config)
    
    # Calculate BBW
    bbw_data = bbw_indicator.calculate_bbw(ohlcv_data)
    
    if bbw_data.empty:
        return False, {}
    
    # This function is called with symbol from the analyzer
    # For now, return the data - actual first-touch detection happens in analyzer
    latest = bbw_data.iloc[-1]
    
    return True, {
        'bbw_data': bbw_data,
        'current_bbw': latest['bbw'],
        'contraction_line': latest['lowest_contraction'],
        'bbw_indicator': bbw_indicator  # Pass indicator instance for first-touch detection
    }
