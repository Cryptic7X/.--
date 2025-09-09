#!/usr/bin/env python3
"""
Enhanced BBW (Bollinger BandWidth) Calculation
Optimized for 15m squeeze detection with adjustable tolerance
"""

import pandas as pd
import numpy as np

def detect_exact_bbw_signals(df, config):
    """
    Enhanced BBW calculation with improved signal detection
    
    Args:
        df: DataFrame with 15m OHLCV data
        config: BBW configuration with tolerance settings
        
    Returns:
        DataFrame with BBW signals and diagnostics
    """
    
    # BBW parameters with fallback defaults
    length = config.get('bbw', {}).get('length', 20)
    mult = config.get('bbw', {}).get('multiplier', 2.0)
    contraction_length = config.get('bbw', {}).get('contraction_length', 125)
    tolerance = config.get('bbw', {}).get('squeeze_tolerance', 0.05)
    
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
    
    # Enhanced squeeze detection with multiple tolerance levels
    squeeze_signal_strict = abs(bbw - lowest_contraction) <= (tolerance * 0.5)  # Very tight
    squeeze_signal_normal = abs(bbw - lowest_contraction) <= tolerance          # Normal
    squeeze_signal_loose = abs(bbw - lowest_contraction) <= (tolerance * 2.0)   # Loose
    
    # Create result DataFrame
    result_df = pd.DataFrame(index=df.index)
    result_df['bbw'] = bbw
    result_df['lowest_contraction'] = lowest_contraction
    result_df['highest_expansion'] = highest_expansion
    result_df['squeezeSignal'] = squeeze_signal_normal  # Use normal as primary
    result_df['squeeze_strength'] = abs(bbw - lowest_contraction)
    result_df['squeeze_ratio'] = bbw / lowest_contraction.replace(0, 1)
    
    # Additional signal levels for diagnostics
    result_df['squeeze_strict'] = squeeze_signal_strict
    result_df['squeeze_loose'] = squeeze_signal_loose
    
    return result_df

def get_latest_squeeze_signal(signals_df):
    """
    Extract latest squeeze signal with enhanced metadata
    
    Returns:
        dict with comprehensive signal data or None
    """
    if signals_df.empty:
        return None
        
    latest = signals_df.iloc[-1]
    
    # Check if any squeeze level is triggered
    has_squeeze = (latest['squeezeSignal'] or 
                  latest.get('squeeze_strict', False) or 
                  latest.get('squeeze_loose', False))
    
    if has_squeeze:
        return {
            'bbw_value': latest['bbw'],
            'lowest_contraction': latest['lowest_contraction'],
            'squeeze_strength': latest['squeeze_strength'],
            'squeeze_ratio': latest.get('squeeze_ratio', 1.0),
            'signal_timestamp': signals_df.index[-1],
            'squeeze_type': get_squeeze_type(latest)
        }
    
    return None

def get_squeeze_type(signal_row):
    """Determine the type of squeeze detected"""
    if signal_row.get('squeeze_strict', False):
        return 'TIGHT'
    elif signal_row['squeezeSignal']:
        return 'NORMAL' 
    elif signal_row.get('squeeze_loose', False):
        return 'LOOSE'
    else:
        return 'NONE'

def validate_bbw_data(df, min_candles=125):
    """Validate DataFrame has sufficient data for BBW calculation"""
    if df is None or df.empty:
        return False, "No data provided"
        
    if len(df) < min_candles:
        return False, f"Insufficient candles: {len(df)} < {min_candles}"
        
    required_columns = ['close', 'high', 'low', 'open']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
        
    # Check for valid price data
    if df['close'].isna().all() or (df['close'] <= 0).all():
        return False, "Invalid price data"
        
    return True, "Valid data"
