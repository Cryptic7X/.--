"""
EXACT CipherB Implementation - Matches Your Pine Script 100%
Your original file - NO WRAPPER, NO CHANGES!
"""

import pandas as pd
import numpy as np

def ema(series, length):
    """Exponential Moving Average - matches Pine Script ta.ema()"""
    return series.ewm(span=length, adjust=False).mean()

def sma(series, length):
    """Simple Moving Average - matches Pine Script ta.sma()"""
    return series.rolling(window=length).mean()

def detect_exact_cipherb_signals(df, config):
    """
    EXACT replication of your Pine Script CipherB logic - YOUR ORIGINAL FUNCTION
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

# SIMPLE DataFrame conversion function - minimal changes to your exact logic
def convert_ohlcv_to_dataframe(ohlcv_data):
    """Convert OHLCV dict to DataFrame for your exact CipherB function"""
    try:
        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(ohlcv_data['timestamp'], unit='ms'),
            'open': pd.to_numeric(ohlcv_data['open'], errors='coerce'),
            'high': pd.to_numeric(ohlcv_data['high'], errors='coerce'),
            'low': pd.to_numeric(ohlcv_data['low'], errors='coerce'), 
            'close': pd.to_numeric(ohlcv_data['close'], errors='coerce'),
            'volume': pd.to_numeric(ohlcv_data['volume'], errors='coerce')
        })
        
        # Set timestamp as index
        df.set_index('timestamp', inplace=True)
        
        # Fill any NaN values
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        return df
        
    except Exception as e:
        print(f"❌ DataFrame conversion failed: {e}")
        return pd.DataFrame()
