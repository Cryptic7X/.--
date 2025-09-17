"""
BBW (Bollinger BandWidth) Exact Pine Script Replication
Matches TradingView BBW indicator exactly
"""

import pandas as pd
import numpy as np

def calculate_bbw(df, length=20, mult=2.0, expansion_length=125, contraction_length=125):
    """
    Calculate BBW exactly as per Pine Script:
    
    //@version=6
    indicator(title="Bollinger BandWidth", shorttitle="BBW")
    length = input.int(20, minval = 1)
    src = input(close, title = "Source")
    mult = input.float(2.0, minval = 0.001, maxval = 50, title = "StdDev")
    expansionLengthInput = input.int(125, "Highest Expansion Length")
    contractionLengthInput = input.int(125, "Lowest Contraction Length")
    
    basis = ta.sma(src, length)
    dev = mult * ta.stdev(src, length)
    upper = basis + dev
    lower = basis - dev
    bbw = ((upper - lower) / basis) * 100
    """
    
    if len(df) < max(length, contraction_length):
        return None
        
    try:
        # Get close prices
        close = df['close'].astype(float)
        
        # Calculate basis (SMA)
        basis = close.rolling(window=length).mean()
        
        # Calculate standard deviation
        std_dev = close.rolling(window=length).std()
        
        # Calculate Bollinger Bands
        dev = mult * std_dev
        upper = basis + dev
        lower = basis - dev
        
        # Calculate BBW: ((upper - lower) / basis) * 100
        bbw = ((upper - lower) / basis) * 100
        
        # Calculate highest expansion and lowest contraction
        highest_expansion = bbw.rolling(window=expansion_length).max()
        lowest_contraction = bbw.rolling(window=contraction_length).min()
        
        # Get latest values
        current_bbw = bbw.iloc[-1]
        current_lowest_contraction = lowest_contraction.iloc[-1]
        current_highest_expansion = highest_expansion.iloc[-1]
        
        # Check for valid values
        if pd.isna(current_bbw) or pd.isna(current_lowest_contraction):
            return None
            
        return {
            'bbw': float(current_bbw),
            'lowest_contraction': float(current_lowest_contraction),
            'highest_expansion': float(current_highest_expansion),
            'basis': float(basis.iloc[-1]),
            'upper_band': float(upper.iloc[-1]),
            'lower_band': float(lower.iloc[-1])
        }
        
    except Exception as e:
        print(f"BBW calculation error: {e}")
        return None

def detect_bbw_squeeze(bbw_data, tolerance=0.01):
    """
    Detect BBW squeeze when BBW touches lowest contraction line
    
    Args:
        bbw_data: Result from calculate_bbw()
        tolerance: Acceptable difference for "touching" (default 0.01)
    
    Returns:
        dict: Squeeze detection results
    """
    if not bbw_data:
        return {'is_squeeze': False, 'reason': 'No BBW data'}
    
    try:
        bbw_value = bbw_data['bbw']
        lowest_contraction = bbw_data['lowest_contraction']
        
        # Calculate difference
        difference = abs(bbw_value - lowest_contraction)
        
        # Check if BBW touches lowest contraction within tolerance
        is_squeeze = difference <= tolerance
        
        # Determine squeeze strength
        if difference <= 0.005:
            strength = "EXTREME"
        elif difference <= 0.01:
            strength = "HIGH"
        else:
            strength = "MODERATE"
        
        return {
            'is_squeeze': is_squeeze,
            'bbw_value': bbw_value,
            'lowest_contraction': lowest_contraction,
            'difference': difference,
            'strength': strength if is_squeeze else None,
            'reason': f"BBW {bbw_value:.4f} vs Lowest {lowest_contraction:.4f} (diff: {difference:.6f})"
        }
        
    except Exception as e:
        return {'is_squeeze': False, 'reason': f'Calculation error: {e}'}
