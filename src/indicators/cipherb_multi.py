"""
CipherB Multi-Timeframe Indicator
EXACT replication of your Pine Script plot shape buy/sell signals
Supports both 2H and 8H analysis with identical logic
"""
import pandas as pd
import numpy as np
import json
import os
import time
from typing import Dict, List, Tuple, Optional

class CipherBMultiIndicator:
    def __init__(self):
        self.cache_file = "cache/cipherb_multi_alerts.json"
        
        # YOUR EXACT Pine Script parameters - NEVER CHANGE
        self.wtChannelLen = 9
        self.wtAverageLen = 12  
        self.wtMALen = 3
        self.osLevel2 = -60
        self.obLevel2 = 60

    def ema(self, series, length):
        """Exponential Moving Average - matches Pine Script ta.ema"""
        return series.ewm(span=length, adjust=False).mean()

    def sma(self, series, length):
        """Simple Moving Average - matches Pine Script ta.sma"""
        return series.rolling(window=length).mean()

    def detect_exact_cipherb_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        100% EXACT replication of your Pine Script CipherB logic
        Using your exact Pine Script parameters and formulas
        """
        # YOUR EXACT Pine Script parameters - hardcoded from your script
        wtChannelLen = 9
        wtAverageLen = 12
        wtMALen = 3
        osLevel2 = -60
        obLevel2 = 60

        # Calculate HLC3 - your exact calculation
        hlc3 = (df['high'] + df['low'] + df['close']) / 3
        
        # YOUR EXACT fwavetrend function implementation
        tfsrc = hlc3
        esa = self.ema(tfsrc, wtChannelLen)  # ta.ema(tfsrc, chlen)
        de = self.ema(abs(tfsrc - esa), wtChannelLen)  # ta.ema(math.abs(tfsrc - esa), chlen)
        ci = (tfsrc - esa) / (0.015 * de)  # (tfsrc - esa) / (0.015 * de)
        
        wtf1 = self.ema(ci, wtAverageLen)  # ta.ema(ci, avg)
        wtf2 = self.sma(wtf1, wtMALen)    # ta.sma(wtf1, malen)
        
        # YOUR EXACT assignments
        wt1 = wtf1
        wt2 = wtf2
        
        # Create results DataFrame
        signals = pd.DataFrame(index=df.index)
        signals['wt1'] = wt1
        signals['wt2'] = wt2
        
        # YOUR EXACT signal conditions
        wtOversold = (wt1 <= osLevel2) & (wt2 <= osLevel2)  # wt1 <= -60 and wt2 <= -60
        wtOverbought = (wt2 >= obLevel2) & (wt1 >= obLevel2)  # wt2 >= 60 and wt1 >= 60
        
        wt1_prev = wt1.shift(1)
        wt2_prev = wt2.shift(1)
        wtCross = ((wt1 > wt2) & (wt1_prev <= wt2_prev)) | ((wt1 < wt2) & (wt1_prev >= wt2_prev))  # ta.cross(wt1, wt2)
        
        wtCrossUp = (wt2 - wt1) <= 0    # wtCrossUp = wt2 - wt1 <= 0
        wtCrossDown = (wt2 - wt1) >= 0  # wtCrossDown = wt2 - wt1 >= 0
        
        # YOUR EXACT plot shape signals
        signals['buySignal'] = wtCross & wtCrossUp & wtOversold      # buySignal = wtCross and wtCrossUp and wtOversold
        signals['sellSignal'] = wtCross & wtCrossDown & wtOverbought # sellSignal = wtCross and wtCrossDown and wtOverbought
        
        return signals

    def load_cache(self) -> Dict:
        """Load multi-timeframe cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_cache(self, cache_data: Dict):
        """Save multi-timeframe cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def analyze_timeframe(self, ohlcv_data: Dict, symbol: str, timeframe: str) -> Dict:
        """Analyze single timeframe using your exact CipherB logic"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame({
                'timestamp': ohlcv_data['timestamp'],
                'open': ohlcv_data['open'],
                'high': ohlcv_data['high'], 
                'low': ohlcv_data['low'],
                'close': ohlcv_data['close'],
                'volume': ohlcv_data['volume']
            })
            
            if len(df) < 50:  # Need enough data for CipherB
                return {'has_signal': False, 'signal_type': None}
            
            # Run your EXACT CipherB analysis
            signals = self.detect_exact_cipherb_signals(df)
            
            # Check for signals in current candle (latest)
            latest_buy = signals['buySignal'].iloc[-1]
            latest_sell = signals['sellSignal'].iloc[-1]
            
            if latest_buy:
                return {'has_signal': True, 'signal_type': 'buy'}
            elif latest_sell:
                return {'has_signal': True, 'signal_type': 'sell'}
            else:
                return {'has_signal': False, 'signal_type': None}
                
        except Exception as e:
            print(f"❌ CipherB analysis error for {symbol} {timeframe}: {e}")
            return {'has_signal': False, 'signal_type': None}

    def analyze_multi_timeframe(self, ohlcv_2h: Dict, ohlcv_8h: Dict, symbol: str) -> Dict:
        """
        Multi-timeframe analysis with your exact logic:
        1. First 2h signal → Always alert
        2. Second same-direction 2h signal → Alert + Add to 8h monitoring
        3. Subsequent same-direction 2h signals → Check 8h confirmation
        4. Opposite direction 2h signal → Reset + Alert
        """
        current_time = time.time()
        cache = self.load_cache()
        
        # Analyze 2h timeframe
        result_2h = self.analyze_timeframe(ohlcv_2h, symbol, '2h')
        
        if not result_2h['has_signal']:
            return {'alert': False, 'alert_type': None, 'message': None}
        
        signal_2h = result_2h['signal_type']  # 'buy' or 'sell'
        
        # Get current cache state for this symbol
        symbol_cache = cache.get(symbol, {
            'last_signal': None,
            'signal_count': 0,
            'monitoring_8h': False,
            'monitoring_direction': None,
            'last_2h_time': 0,
            'last_8h_time': 0
        })
        
        # Determine alert logic
        if symbol_cache['last_signal'] != signal_2h:
            # Different direction OR first signal ever → RESET + ALERT
            alert_type = "2H_SIGNAL"
            alert_message = f"2H SIGNAL: {symbol} {signal_2h.upper()}"
            
            # Update cache - reset state
            symbol_cache = {
                'last_signal': signal_2h,
                'signal_count': 1,
                'monitoring_8h': False,
                'monitoring_direction': None,
                'last_2h_time': current_time,
                'last_8h_time': 0
            }
            
            cache[symbol] = symbol_cache
            self.save_cache(cache)
            
            return {
                'alert': True,
                'alert_type': alert_type,
                'message': alert_message,
                'signal_type': signal_2h
            }
        
        else:
            # Same direction as last signal
            if symbol_cache['signal_count'] == 1:
                # Second same-direction signal → REPEATED + ADD TO MONITORING
                alert_type = "2H_REPEATED"
                alert_message = f"2H REPEATED: {symbol} {signal_2h.upper()} (2nd same-direction signal)"
                
                # Update cache - enter monitoring mode
                symbol_cache['signal_count'] = 2
                symbol_cache['monitoring_8h'] = True
                symbol_cache['monitoring_direction'] = signal_2h
                symbol_cache['last_2h_time'] = current_time
                
                cache[symbol] = symbol_cache
                self.save_cache(cache)
                
                return {
                    'alert': True,
                    'alert_type': alert_type,
                    'message': alert_message,
                    'signal_type': signal_2h
                }
            
            elif symbol_cache['monitoring_8h']:
                # Third+ same-direction signal → CHECK 8H CONFIRMATION
                if not ohlcv_8h:
                    return {'alert': False, 'alert_type': None, 'message': None}
                
                # Analyze 8h timeframe for confirmation
                result_8h = self.analyze_timeframe(ohlcv_8h, symbol, '8h')
                
                if result_8h['has_signal'] and result_8h['signal_type'] == signal_2h:
                    # 8h confirms → CONFIRMED SIGNAL
                    alert_type = "2H_8H_CONFIRMED"
                    alert_message = f"2H+8H CONFIRMED: {symbol} {signal_2h.upper()}"
                    
                    # Update cache
                    symbol_cache['signal_count'] += 1
                    symbol_cache['last_2h_time'] = current_time
                    symbol_cache['last_8h_time'] = current_time
                    
                    cache[symbol] = symbol_cache
                    self.save_cache(cache)
                    
                    return {
                        'alert': True,
                        'alert_type': alert_type,
                        'message': alert_message,
                        'signal_type': signal_2h
                    }
                else:
                    # 8h doesn't confirm → SILENT
                    # Update cache count but no alert
                    symbol_cache['signal_count'] += 1
                    symbol_cache['last_2h_time'] = current_time
                    
                    cache[symbol] = symbol_cache
                    self.save_cache(cache)
                    
                    return {'alert': False, 'alert_type': None, 'message': None}
            
            else:
                # Should not reach here, but handle gracefully
                return {'alert': False, 'alert_type': None, 'message': None}
