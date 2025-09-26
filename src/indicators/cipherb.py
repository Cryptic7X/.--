"""
EXACT CipherB Implementation - 100% MATCHES YOUR PINE SCRIPT
Multi-timeframe (2H + 8H) with PINPOINT TIMING ACCURACY
"""
import pandas as pd
import numpy as np
import json
import os
import time
from typing import Dict, List, Optional
from datetime import datetime, timezone

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
    wtChannelLen = 9
    wtAverageLen = 12  
    wtMALen = 3
    osLevel2 = -60
    obLevel2 = 60
    
    # Calculate HLC3 - your exact 
    hlc3 = (df['high'] + df['low'] + df['close']) / 3
    
    # YOUR EXACT wtMASource
    tfsrc = hlc3
    
    # EXACT replication of your Pine Script fWavetrend function
    esa = ema(tfsrc, wtChannelLen)  # ta.ema(tfsrc, chlen)
    de = ema(abs(tfsrc - esa), wtChannelLen)  # ta.ema(math.abs(tfsrc - esa), chlen)
    ci = (tfsrc - esa) / (0.015 * de)  # (tfsrc - esa) / (0.015 * de)
    
    wtf1 = ema(ci, wtAverageLen)  # ta.ema(ci, avg)
    wtf2 = sma(wtf1, wtMALen)     # ta.sma(wtf1, malen)
    
    # YOUR EXACT assignments
    wt1 = wtf1
    wt2 = wtf2
    
    # Create results DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2
    
    # YOUR EXACT conditions
    wtOversold = (wt1 <= osLevel2) & (wt2 <= osLevel2)  # wt1 <= -60 and wt2 <= -60
    wtOverbought = (wt2 >= obLevel2) & (wt1 >= obLevel2)  # wt2 >= 60 and wt1 >= 60
    
    # Calculate crossovers - YOUR EXACT logic
    wt1_prev = wt1.shift(1)
    wt2_prev = wt2.shift(1) 
    
    wtCross = ((wt1 > wt2) & (wt1_prev <= wt2_prev)) | ((wt1 < wt2) & (wt1_prev >= wt2_prev))  # ta.cross(wt1, wt2)
    wtCrossUp = (wt2 - wt1) <= 0  # wt2 - wt1 <= 0
    wtCrossDown = (wt2 - wt1) >= 0  # wt2 - wt1 >= 0
    
    # YOUR EXACT plot shape conditions
    signals['buySignal'] = wtCross & wtCrossUp & wtOversold    # buySignal = wtCross and wtCrossUp and wtOversold
    signals['sellSignal'] = wtCross & wtCrossDown & wtOverbought  # sellSignal = wtCross and wtCrossDown and wtOverbought
    
    # Add timestamp column for signal timing
    signals['timestamp'] = df.index
    
    return signals

def get_timeframe_freshness_window(timeframe: str) -> int:
    """Get freshness window in seconds for each timeframe"""
    if timeframe == '2h':
        return 2 * 60 * 60  # 2 hours in seconds
    elif timeframe == '8h':
        return 8 * 60 * 60  # 8 hours in seconds
    else:
        return 2 * 60 * 60  # Default 2 hours

class CipherBMultiTimeframe:
    """Multi-timeframe CipherB with pinpoint timing accuracy"""
    
    def __init__(self):
        self.cache_file = "cache/cipherb_multi_alerts.json"
        
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
        """Save cache to file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")
    
    def find_fresh_signals(self, signals_df: pd.DataFrame, timeframe: str, current_time: float) -> List[Dict]:
        """
        Find signals that are fresh within the timeframe window
        Returns list of fresh signals with exact timing
        """
        fresh_signals = []
        freshness_window = get_timeframe_freshness_window(timeframe)
        
        try:
            # Check each candle for signals
            for idx in signals_df.index:
                row = signals_df.loc[idx]
                
                # Convert pandas timestamp to unix timestamp
                if hasattr(idx, 'timestamp'):
                    candle_timestamp = idx.timestamp()
                else:
                    # Fallback if timestamp conversion fails
                    candle_timestamp = current_time - (len(signals_df) - signals_df.index.get_loc(idx)) * freshness_window
                
                # Check if signal is within freshness window
                time_diff = current_time - candle_timestamp
                
                if 0 <= time_diff <= freshness_window:
                    # Check for buy signal
                    if row['buySignal']:
                        fresh_signals.append({
                            'signal_type': 'BUY',
                            'candle_time': candle_timestamp,
                            'wt1': round(row['wt1'], 1),
                            'wt2': round(row['wt2'], 1),
                            'time_diff_hours': time_diff / 3600,
                            'is_fresh': True
                        })
                    
                    # Check for sell signal
                    if row['sellSignal']:
                        fresh_signals.append({
                            'signal_type': 'SELL',
                            'candle_time': candle_timestamp,
                            'wt1': round(row['wt1'], 1),
                            'wt2': round(row['wt2'], 1),
                            'time_diff_hours': time_diff / 3600,
                            'is_fresh': True
                        })
            
            return fresh_signals
            
        except Exception as e:
            print(f"❌ Error finding fresh signals: {e}")
            return []
    
    def analyze_timeframe(self, df: pd.DataFrame, timeframe: str, symbol: str) -> Optional[Dict]:
        """Analyze timeframe for fresh CipherB signals only"""
        try:
            if len(df) < 25:
                return None
            
            # Run your EXACT Pine Script logic  
            signals_df = detect_exact_cipherb_signals(df, {
                'wtChannelLen': 9,
                'wtAverageLen': 12, 
                'wtMALen': 3,
                'oversold_threshold': -60,
                'overbought_threshold': 60
            })
            
            if signals_df.empty:
                return None
            
            # Find fresh signals within the timeframe window
            current_time = time.time()
            fresh_signals = self.find_fresh_signals(signals_df, timeframe, current_time)
            
            if not fresh_signals:
                return None
            
            # Return the most recent fresh signal
            latest_signal = max(fresh_signals, key=lambda x: x['candle_time'])
            
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'signal_type': latest_signal['signal_type'],
                'buy_signal': latest_signal['signal_type'] == 'BUY',
                'sell_signal': latest_signal['signal_type'] == 'SELL',
                'wt1': latest_signal['wt1'],
                'wt2': latest_signal['wt2'],
                'candle_time': latest_signal['candle_time'],
                'time_diff_hours': latest_signal['time_diff_hours'],
                'timestamp': current_time,
                'is_fresh': True
            }
            
        except Exception as e:
            print(f"❌ CipherB {timeframe} analysis failed for {symbol}: {e}")
            return None
    
    def determine_alert_action(self, symbol: str, signal_2h: Dict, signal_8h: Optional[Dict] = None) -> Dict:
        """
        Determine what alert to send based on multi-timeframe logic
        Only processes FRESH signals within their respective windows
        """
        cache = self.load_cache()
        current_time = time.time()
        
        signal_type_2h = signal_2h['signal_type']
        cache_key = symbol
        
        # Initialize result
        result = {
            'send_alert': False,
            'alert_type': None,
            'message_type': None,
            'signal_2h': signal_2h,
            'signal_8h': signal_8h
        }
        
        # Get current cache state
        if cache_key in cache:
            last_signal = cache[cache_key]['last_signal']
            signal_count = cache[cache_key]['signal_count']
            monitoring_8h = cache[cache_key]['monitoring_8h']
            last_2h_time = cache[cache_key].get('last_2h_time', 0)
        else:
            last_signal = None
            signal_count = 0
            monitoring_8h = False
            last_2h_time = 0
        
        # CRITICAL: Check if this 2H signal was already processed
        signal_age_hours = signal_2h['time_diff_hours']
        if last_2h_time > 0:
            time_since_last_alert = (current_time - last_2h_time) / 3600
            if time_since_last_alert < 1.8:  # Less than 1.8 hours since last alert
                print(f"🔄 {symbol}: Signal too recent (last alert {time_since_last_alert:.1f}h ago)")
                return result  # Don't process
        
        # LOGIC IMPLEMENTATION WITH FRESH SIGNALS
        
        # Case 1: Direction change (reset)
        if last_signal and last_signal != signal_type_2h.lower():
            result['send_alert'] = True
            result['alert_type'] = signal_type_2h
            result['message_type'] = '2H_SIGNAL'
            
            print(f"✅ {symbol}: Direction change {last_signal.upper()} → {signal_type_2h}")
            
            # Reset cache
            cache[cache_key] = {
                'last_signal': signal_type_2h.lower(),
                'signal_count': 1,
                'monitoring_8h': False,
                'monitoring_direction': None,
                'last_2h_time': current_time,
                'last_8h_time': 0
            }
        
        # Case 2: Same direction signals
        elif not last_signal or last_signal == signal_type_2h.lower():
            
            if signal_count == 0:
                # First signal ever
                result['send_alert'] = True
                result['alert_type'] = signal_type_2h
                result['message_type'] = '2H_SIGNAL'
                
                print(f"✅ {symbol}: First {signal_type_2h} signal")
                
                cache[cache_key] = {
                    'last_signal': signal_type_2h.lower(),
                    'signal_count': 1,
                    'monitoring_8h': False,
                    'monitoring_direction': None,
                    'last_2h_time': current_time,
                    'last_8h_time': 0
                }
                
            elif signal_count == 1:
                # Second same-direction signal
                result['send_alert'] = True
                result['alert_type'] = signal_type_2h
                result['message_type'] = '2H_REPEATED'
                
                print(f"✅ {symbol}: Second {signal_type_2h} signal → 8H monitoring activated")
                
                cache[cache_key] = {
                    'last_signal': signal_type_2h.lower(),
                    'signal_count': 2,
                    'monitoring_8h': True,
                    'monitoring_direction': signal_type_2h.lower(),
                    'last_2h_time': current_time,
                    'last_8h_time': 0
                }
                
            elif signal_count >= 2 and monitoring_8h:
                # 3rd+ signal - check 8h confirmation
                if signal_8h and signal_8h['signal_type'].lower() == signal_type_2h.lower():
                    result['send_alert'] = True
                    result['alert_type'] = signal_type_2h
                    result['message_type'] = '2H_8H_CONFIRMED'
                    
                    print(f"✅ {symbol}: 2H+8H confirmed {signal_type_2h}")
                    
                    cache[cache_key]['last_2h_time'] = current_time
                    cache[cache_key]['last_8h_time'] = current_time
                else:
                    # 8h doesn't confirm - silent
                    result['send_alert'] = False
                    print(f"🔍 {symbol}: 8H no confirmation for {signal_type_2h}")
                    cache[cache_key]['last_2h_time'] = current_time
        
        # Save cache
        self.save_cache(cache)
        return result
