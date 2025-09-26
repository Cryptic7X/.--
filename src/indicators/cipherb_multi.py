"""
CipherB Multi-Timeframe Indicator - PRESERVES YOUR EXACT PINE SCRIPT LOGIC
2H + 8H analysis with smart cache management
"""
import pandas as pd
import numpy as np
import json
import os
import time
from typing import Dict, Tuple, Optional

def ema(series, length):
    """Exponential Moving Average - matches Pine Script ta.ema()"""
    return series.ewm(span=length, adjust=False).mean()

def sma(series, length):
    """Simple Moving Average - matches Pine Script ta.sma()"""
    return series.rolling(window=length).mean()

def detect_exact_cipherb_signals(df, config):
    """
    100% EXACT replication of your Pine Script CipherB logic
    Using your exact Pine Script parameters and formulas
    """
    # YOUR EXACT Pine Script parameters - hardcoded from your script
    wtChannelLen = 9  # wtChannelLen = 9
    wtAverageLen = 12  # wtAverageLen = 12
    wtMALen = 3  # wtMALen = 3
    osLevel2 = -60  # osLevel2 = -60
    obLevel2 = 60  # obLevel2 = 60

    # Calculate HLC3 - your exact: wtMASource = hlc3
    hlc3 = (df['high'] + df['low'] + df['close']) / 3

    # YOUR EXACT f_wavetrend function implementation
    tfsrc = hlc3
    esa = ema(tfsrc, wtChannelLen)  # ta.ema(tfsrc, chlen)
    de = ema(abs(tfsrc - esa), wtChannelLen)  # ta.ema(math.abs(tfsrc - esa), chlen)
    ci = (tfsrc - esa) / (0.015 * de)  # (tfsrc - esa) / (0.015 * de)
    wtf1 = ema(ci, wtAverageLen)  # ta.ema(ci, avg)
    wtf2 = sma(wtf1, wtMALen)  # ta.sma(wtf1, malen)

    # YOUR EXACT assignments
    wt1 = wtf1  # wt1 = wtf1
    wt2 = wtf2  # wt2 = wtf2

    # Create results DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2

    # YOUR EXACT Pine Script conditions - copied line by line
    # wtOversold = wt1 <= -60 and wt2 <= -60
    wtOversold = (wt1 <= osLevel2) & (wt2 <= osLevel2)

    # wtOverbought = wt2 >= 60 and wt1 >= 60
    wtOverbought = (wt2 >= obLevel2) & (wt1 >= obLevel2)

    # wtCross = ta.cross(wt1, wt2)
    wt1_prev = wt1.shift(1)
    wt2_prev = wt2.shift(1)
    wtCross = ((wt1 > wt2) & (wt1_prev <= wt2_prev)) | ((wt1 < wt2) & (wt1_prev >= wt2_prev))

    # wtCrossUp = wt2 - wt1 <= 0
    wtCrossUp = (wt2 - wt1) <= 0

    # wtCrossDown = wt2 - wt1 >= 0
    wtCrossDown = (wt2 - wt1) >= 0

    # YOUR EXACT Pine Script signal logic
    # buySignal = wtCross and wtCrossUp and wtOversold
    signals['buySignal'] = wtCross & wtCrossUp & wtOversold

    # sellSignal = wtCross and wtCrossDown and wtOverbought
    signals['sellSignal'] = wtCross & wtCrossDown & wtOverbought

    return signals


class CipherBMultiIndicator:
    def __init__(self):
        self.cache_file = "cache/cipherb_multi_alerts.json"

    def load_cache(self) -> Dict:
        """Load cache from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_cache(self, cache_data: Dict):
        """Save cache to file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def analyze_timeframe(self, df: pd.DataFrame, timeframe: str) -> Optional[Dict]:
        """Analyze single timeframe using YOUR EXACT logic"""
        try:
            if len(df) < 25:
                return None

            # Run your EXACT Pine Script logic (unchanged)
            signals_df = detect_exact_cipherb_signals(df, {
                'wt_channel_len': 9,
                'wt_average_len': 12,
                'wt_ma_len': 3,
                'oversold_threshold': -60,
                'overbought_threshold': 60
            })

            if signals_df.empty:
                return None

            # Check penultimate candle (your exact logic)
            penultimate = signals_df.iloc[-2]
            
            buy_signal = penultimate["buySignal"]
            sell_signal = penultimate["sellSignal"]

            if not buy_signal and not sell_signal:
                return None

            return {
                'signal_type': 'BUY' if buy_signal else 'SELL',
                'wt1': round(penultimate['wt1'], 1),
                'wt2': round(penultimate['wt2'], 1),
                'timeframe': timeframe,
                'timestamp': time.time()
            }

        except Exception as e:
            return None

    def determine_alert_type(self, symbol: str, signal_2h: Dict) -> Tuple[bool, str, str]:
        """
        Determine what type of alert to send based on cache state
        Returns: (should_alert, alert_type, message_type)
        """
        cache = self.load_cache()
        current_time = time.time()
        signal_type = signal_2h['signal_type']

        # Get coin state from cache
        if symbol not in cache:
            # State 1: First signal ever
            cache[symbol] = {
                'last_signal': signal_type.lower(),
                'signal_count': 1,
                'monitoring_8h': False,
                'monitoring_direction': None,
                'last_2h_time': current_time,
                'last_8h_time': 0
            }
            self.save_cache(cache)
            return True, "2H SIGNAL", f"2H SIGNAL: {symbol} {signal_type}"

        coin_state = cache[symbol]
        last_signal = coin_state.get('last_signal', '')
        signal_count = coin_state.get('signal_count', 0)
        monitoring_8h = coin_state.get('monitoring_8h', False)

        if signal_type.lower() != last_signal:
            # State 4: Direction change - RESET
            cache[symbol] = {
                'last_signal': signal_type.lower(),
                'signal_count': 1,
                'monitoring_8h': False,
                'monitoring_direction': None,
                'last_2h_time': current_time,
                'last_8h_time': 0
            }
            self.save_cache(cache)
            return True, "2H SIGNAL", f"2H SIGNAL: {symbol} {signal_type}"

        # Same direction signal
        if signal_count == 1:
            # State 2: Second same-direction signal
            cache[symbol].update({
                'signal_count': 2,
                'monitoring_8h': True,
                'monitoring_direction': signal_type.lower(),
                'last_2h_time': current_time
            })
            self.save_cache(cache)
            return True, "2H REPEATED", f"2H REPEATED: {symbol} {signal_type} (2nd same-direction signal)"

        # State 3: Third+ same-direction signal - need 8h confirmation
        if monitoring_8h:
            cache[symbol]['last_2h_time'] = current_time
            self.save_cache(cache)
            return False, "8H PENDING", f"Monitoring 8H for {symbol} {signal_type} confirmation"

        return False, "NO ALERT", "Unknown state"

    def check_8h_confirmation(self, symbol: str, signal_8h: Optional[Dict]) -> Tuple[bool, str]:
        """Check if 8h signal confirms the 2h direction"""
        cache = self.load_cache()
        
        if symbol not in cache:
            return False, "No cache entry"

        coin_state = cache[symbol]
        monitoring_direction = coin_state.get('monitoring_direction', '')
        
        if not signal_8h:
            return False, f"No 8H signal for {symbol}"

        signal_8h_type = signal_8h['signal_type'].lower()
        
        if signal_8h_type == monitoring_direction:
            # 8h confirms 2h direction
            coin_state['last_8h_time'] = time.time()
            self.save_cache(cache)
            return True, f"2H+8H CONFIRMED: {symbol} {signal_8h['signal_type']}"
        
        return False, f"8H {signal_8h_type} doesn't match monitoring {monitoring_direction}"

    def get_monitoring_coins(self) -> Dict:
        """Get coins that need 8h monitoring"""
        cache = self.load_cache()
        monitoring_coins = {}
        
        for symbol, state in cache.items():
            if state.get('monitoring_8h', False):
                monitoring_coins[symbol] = state
        
        return monitoring_coins
