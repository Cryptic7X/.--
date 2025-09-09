#!/usr/bin/env python3
"""
BBW (Bollinger BandWidth) 15-Minute Exact Calculation
Optimized for fast 15m squeeze detection
"""

import pandas as pd
import numpy as np

def detect_exact_bbw_signals(df, config):
    """
    Exact 15m BBW calculation matching Pine Script
    
    Args:
        df: DataFrame with 15m OHLCV data
        config: BBW configuration
        
    Returns:
        DataFrame with BBW signals
    """
    
    # BBW parameters
    length = config['bbw']['length']
    mult = config['bbw']['multiplier']
    contraction_length = config['bbw']['contraction_length']
    tolerance = config['bbw']['squeeze_tolerance']
    
    # Use close price
    close = df['close']
    
    # Bollinger Bands calculation
    basis = close.rolling(window=length).mean()
    std_dev = close.rolling(window=length).std()
    
    upper = basis + (mult * std_dev)
    lower = basis - (mult * std_dev)
    
    # BBW calculation: ((upper - lower) / basis) * 100
    bbw = ((upper - lower) / basis) * 100
    
    # Rolling contraction/expansion levels
    highest_expansion = bbw.rolling(window=contraction_length).max()
    lowest_contraction = bbw.rolling(window=contraction_length).min()
    
    # 15m squeeze detection: BBW touches lowest contraction
    squeeze_signal = abs(bbw - lowest_contraction) <= tolerance
    
    # Create result DataFrame
    result_df = pd.DataFrame(index=df.index)
    result_df['bbw'] = bbw
    result_df['lowest_contraction'] = lowest_contraction
    result_df['highest_expansion'] = highest_expansion
    result_df['squeezeSignal'] = squeeze_signal
    result_df['squeeze_strength'] = abs(bbw - lowest_contraction)
    
    return result_df

def get_latest_squeeze_signal(signals_df):
    """Extract latest 15m squeeze signal"""
    if signals_df.empty:
        return None
        
    latest = signals_df.iloc[-1]
    
    if latest['squeezeSignal']:
        return {
            'bbw_value': latest['bbw'],
            'lowest_contraction': latest['lowest_contraction'],
            'squeeze_strength': latest['squeeze_strength'],
            'signal_timestamp': signals_df.index[-1]
        }
    
    return None
