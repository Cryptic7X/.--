"""
CipherB EXACT Wrapper - Converts OHLCV Dict to proper DataFrame for your EXACT CipherB function
This preserves your 100% working CipherB logic while fixing the data conversion
"""

import pandas as pd
import numpy as np
import sys
import os

# Your EXACT CipherB functions (no changes to these!)
def ema(series, length):
    """Exponential Moving Average - matches Pine Script ta.ema()"""
    return series.ewm(span=length, adjust=False).mean()

def sma(series, length):
    """Simple Moving Average - matches Pine Script ta.sma()"""
    return series.rolling(window=length).mean()

def detect_exact_cipherb_signals(df, config):
    """
    Your EXACT CipherB logic - NO CHANGES TO THIS FUNCTION!
    """
    # Your exact Pine Script parameters
    wtChannelLen = config.get('wt_channel_len', 9)
    wtAverageLen = config.get('wt_average_len', 12)
    wtMALen = config.get('wt_ma_len', 3)
    osLevel2 = config.get('oversold_threshold', -60)
    obLevel2 = config.get('overbought_threshold', 60)

    # Calculate HLC3 (wtMASource = hlc3 in your script)
    hlc3 = (df['high'] + df['low'] + df['close']) / 3

    # WaveTrend calculation - EXACT match to your Pine Script f_wavetrend function
    esa = ema(hlc3, wtChannelLen) # ta.ema(tfsrc, chlen)
    de = ema(abs(hlc3 - esa), wtChannelLen) # ta.ema(math.abs(tfsrc - esa), chlen)
    ci = (hlc3 - esa) / (0.015 * de) # (tfsrc - esa) / (0.015 * de)
    wt1 = ema(ci, wtAverageLen) # ta.ema(ci, avg) = wtf1
    wt2 = sma(wt1, wtMALen) # ta.sma(wt1, malen) = wtf2

    # Create results DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2

    # EXACT Pine Script conditions
    # wtCross = ta.cross(wt1, wt2)
    wt1_prev = wt1.shift(1)
    wt2_prev = wt2.shift(1)
    wtCross = ((wt1 > wt2) & (wt1_prev <= wt2_prev)) | ((wt1 < wt2) & (wt1_prev >= wt2_prev))

    # wtCrossUp = wt2 - wt1 <= 0
    wtCrossUp = (wt2 - wt1) <= 0

    # wtCrossDown = wt2 - wt1 >= 0
    wtCrossDown = (wt2 - wt1) >= 0

    # wtOversold = wt1 <= -60 and wt2 <= -60
    wtOversold = (wt1 <= osLevel2) & (wt2 <= osLevel2)

    # wtOverbought = wt2 >= 60 and wt1 >= 60
    wtOverbought = (wt2 >= obLevel2) & (wt1 >= obLevel2)

    # EXACT Pine Script signal logic
    signals['buySignal'] = wtCross & wtCrossUp & wtOversold
    signals['sellSignal'] = wtCross & wtCrossDown & wtOverbought

    return signals

def detect_cipherb_signals_2h(ohlcv_data: dict, config: dict) -> pd.DataFrame:
    """
    FIXED: Proper OHLCV dict to DataFrame conversion for your exact CipherB function
    """
    try:
        # CRITICAL FIX: Ensure data is properly structured
        if not ohlcv_data or not isinstance(ohlcv_data, dict):
            print("❌ Invalid OHLCV data format")
            return pd.DataFrame()
        
        # Check required keys
        required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        for key in required_keys:
            if key not in ohlcv_data:
                print(f"❌ Missing key in OHLCV data: {key}")
                return pd.DataFrame()
        
        # CRITICAL FIX: Convert lists to proper arrays and ensure equal lengths
        try:
            timestamp_data = list(ohlcv_data['timestamp'])
            open_data = list(ohlcv_data['open'])
            high_data = list(ohlcv_data['high'])
            low_data = list(ohlcv_data['low'])
            close_data = list(ohlcv_data['close'])
            volume_data = list(ohlcv_data['volume'])
            
            # Check lengths match
            lengths = [len(timestamp_data), len(open_data), len(high_data), 
                      len(low_data), len(close_data), len(volume_data)]
            
            if len(set(lengths)) != 1:
                print(f"❌ OHLCV data length mismatch: {lengths}")
                return pd.DataFrame()
            
            data_length = lengths[0]
            if data_length < 50:
                print(f"❌ Insufficient data: {data_length} candles (need 50+)")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Data validation failed: {e}")
            return pd.DataFrame()
        
        # CRITICAL FIX: Create DataFrame with proper data types
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(timestamp_data, unit='ms'),
            'open': pd.to_numeric(open_data, errors='coerce'),
            'high': pd.to_numeric(high_data, errors='coerce'),
            'low': pd.to_numeric(low_data, errors='coerce'), 
            'close': pd.to_numeric(close_data, errors='coerce'),
            'volume': pd.to_numeric(volume_data, errors='coerce')
        })
        
        # Check for NaN values
        if df.isnull().any().any():
            print("⚠️ Found NaN values in data, filling with previous values")
            df = df.fillna(method='ffill').fillna(method='bfill')
        
        # Set timestamp as index
        df.set_index('timestamp', inplace=True)
        
        # Ensure we still have enough data after processing
        if len(df) < 50:
            print(f"❌ Insufficient data after processing: {len(df)} candles")
            return pd.DataFrame()

        # Call your EXACT CipherB function with proper DataFrame
        return detect_exact_cipherb_signals(df, config)

    except Exception as e:
        print(f"❌ CipherB DataFrame conversion failed: {str(e)}")
        return pd.DataFrame()

# Additional helper function to debug data format
def debug_ohlcv_data(ohlcv_data, symbol):
    """Debug function to check OHLCV data format"""
    print(f"🔍 DEBUG {symbol} OHLCV data:")
    print(f"   Type: {type(ohlcv_data)}")
    
    if isinstance(ohlcv_data, dict):
        for key in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
            if key in ohlcv_data:
                data = ohlcv_data[key]
                print(f"   {key}: type={type(data)}, len={len(data) if hasattr(data, '__len__') else 'N/A'}")
                if hasattr(data, '__len__') and len(data) > 0:
                    print(f"      first={data[0]}, type={type(data[0])}")
            else:
                print(f"   {key}: MISSING")
    else:
        print(f"   Not a dict! Keys: {list(ohlcv_data.keys()) if hasattr(ohlcv_data, 'keys') else 'No keys'}")
