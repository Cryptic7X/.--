#!/usr/bin/env python3
"""
BBW (Bollinger BandWidth) Exact Calculation
Exact Pine Script replication for BBW squeeze detection
"""

import pandas as pd
import numpy as np

def detect_exact_bbw_signals(df, config):
    """
    Exact BBW calculation matching Pine Script parameters
    
    Args:
        df: DataFrame with OHLCV data (columns: open, high, low, close, volume)
        config: BBW configuration from config.yaml
        
    Returns:
        DataFrame with BBW signals and metadata
    """
    
    # Extract BBW parameters
    length = config['bbw']['length']
    mult = config['bbw']['multiplier']
    contraction_length = config['bbw']['contraction_length']
    tolerance = config['bbw']['squeeze_tolerance']
    
    # Use close price for calculation
    close = df['close']
    
    # Bollinger Bands calculation
    basis = close.rolling(window=length).mean()
    std_dev = close.rolling(window=length).std()
    
    upper = basis + (mult * std_dev)
    lower = basis - (mult * std_dev)
    
    # BBW calculation: ((upper - lower) / basis) * 100
    bbw = ((upper - lower) / basis) * 100
    
    # Rolling highest expansion and lowest contraction over specified length
    highest_expansion = bbw.rolling(window=contraction_length).max()
    lowest_contraction = bbw.rolling(window=contraction_length).min()
    
    # Squeeze signal detection: BBW touches lowest contraction line
    squeeze_signal = abs(bbw - lowest_contraction) <= tolerance
    
    # Create result DataFrame with same index as input
    result_df = pd.DataFrame(index=df.index)
    result_df['bbw'] = bbw
    result_df['lowest_contraction'] = lowest_contraction
    result_df['highest_expansion'] = highest_expansion
    result_df['squeezeSignal'] = squeeze_signal
    
    # Additional metadata for alerts
    result_df['squeeze_strength'] = abs(bbw - lowest_contraction)
    result_df['expansion_ratio'] = bbw / lowest_contraction
    
    return result_df

def get_latest_squeeze_signal(signals_df):
    """
    Extract latest squeeze signal from BBW signals DataFrame
    
    Returns:
        dict with signal metadata or None if no signal
    """
    if signals_df.empty:
        return None
        
    latest = signals_df.iloc[-1]
    
    if latest['squeezeSignal']:
        return {
            'bbw_value': latest['bbw'],
            'lowest_contraction': latest['lowest_contraction'],
            'squeeze_strength': latest['squeeze_strength'],
            'expansion_ratio': latest['expansion_ratio'],
            'signal_timestamp': signals_df.index[-1]
        }
    
    return None
