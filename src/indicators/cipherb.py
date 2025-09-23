"""
EXACT CipherB Implementation - 100% MATCHES YOUR PINE SCRIPT
Using your exact parameters and logic
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
    100% EXACT replication of your Pine Script CipherB logic
    """
    # YOUR EXACT Pine Script parameters from the code
    wtChannelLen = 9      # YOUR EXACT VALUE
    wtAverageLen = 12     # YOUR EXACT VALUE  
    wtMALen = 3           # YOUR EXACT VALUE
    osLevel2 = -60        # YOUR EXACT VALUE
    obLevel2 = 60         # YOUR EXACT VALUE

    # Calculate HLC3 (wtMASource = hlc3 in your script)
    hlc3 = (df['high'] + df['low'] + df['close']) / 3

    # YOUR EXACT WaveTrend calculation - f_wavetrend function
    tfsrc = hlc3
    esa = ema(tfsrc, wtChannelLen)  # ta.ema(tfsrc, chlen)
    de = ema(abs(tfsrc - esa), wtChannelLen)  # ta.ema(math.abs(tfsrc - esa), chlen)
    ci = (tfsrc - esa) / (0.015 * de)  # (tfsrc - esa) / (0.015 * de)
    wtf1 = ema(ci, wtAverageLen)  # ta.ema(ci, avg)
    wtf2 = sma(wtf1, wtMALen)  # ta.sma(wtf1, malen)
    
    wt1 = wtf1  # YOUR EXACT ASSIGNMENT
    wt2 = wtf2  # YOUR EXACT ASSIGNMENT

    # Create results DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2

    # YOUR EXACT Pine Script conditions - LINE BY LINE
    # wtOversold = wt1 <= -60 and wt2 <= -60
    wtOversold = (wt1 <= osLevel2) & (wt2 <= osLevel2)
    
    # wtOverbought = wt2 >= 60 and wt1 >= 60  (YOUR EXACT ORDER!)
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
