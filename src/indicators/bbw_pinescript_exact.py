#!/usr/bin/env python3
"""
BBW Bollinger BandWidth Exact Calculation
Exact Pine Script replication for BBW squeeze detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class BBWIndicator:
    def __init__(self, length=20, multiplier=2.0, contraction_length=125):
        self.length = length
        self.multiplier = multiplier
        self.contraction_length = contraction_length
        
    def calculate_bbw_signals(self, df):
        """
        Calculate BBW signals - exact Pine Script match
        basis = ta.sma(src, length)
        dev = mult * ta.stdev(src, length)
        upper = basis + dev
        lower = basis - dev
        bbw = ((upper - lower) / basis) * 100
        """
        close = df['close']
        
        # Bollinger Bands calculation (exact Pine Script match)
        basis = close.rolling(window=self.length).mean()
        std_dev = close.rolling(window=self.length).std()
        upper = basis + (self.multiplier * std_dev)
        lower = basis - (self.multiplier * std_dev)
        
        # BBW calculation
        bbw = ((upper - lower) / basis) * 100
        
        # Rolling highest expansion and lowest contraction over specified length
        highest_expansion = bbw.rolling(window=self.contraction_length).max()
        lowest_contraction = bbw.rolling(window=self.contraction_length).min()
        
        # Create result DataFrame with same index as input
        result_df = pd.DataFrame(index=df.index)
        result_df['bbw'] = bbw
        result_df['lowest_contraction'] = lowest_contraction
        result_df['highest_expansion'] = highest_expansion
        result_df['basis'] = basis
        result_df['upper'] = upper
        result_df['lower'] = lower
        
        return result_df
    
    def get_latest_squeeze_signal(self, signals_df, tolerance=0.01, separation_threshold=0.05):
        """
        Smart squeeze detection - your exact requirements:
        1. Alert when BBW first touches lowest_contraction
        2. No repeated alerts while BBW stays at same level
        3. New alert only when BBW moves away and comes back
        """
        if signals_df.empty:
            return None
        
        # State tracking variables
        currently_touching = False
        last_signal_contraction_level = None
        latest_signal = None
        
        for i in range(len(signals_df)):
            bbw_val = signals_df['bbw'].iloc[i]
            lowest_val = signals_df['lowest_contraction'].iloc[i]
            
            if pd.isna(bbw_val) or pd.isna(lowest_val):
                continue
            
            # Check if BBW is touching lowest contraction (within tolerance)
            difference = abs(bbw_val - lowest_val)
            is_touching = difference <= tolerance
            
            if is_touching:
                if not currently_touching:
                    # BBW just started touching - this is a potential signal
                    
                    # Check if this is a new contraction level (dynamic line)
                    # OR if BBW moved away and came back to same level
                    is_new_signal = (
                        last_signal_contraction_level is None or 
                        abs(lowest_val - last_signal_contraction_level) > separation_threshold
                    )
                    
                    if is_new_signal:
                        # Determine squeeze strength
                        squeeze_strength = self._calculate_squeeze_strength(bbw_val, lowest_val)
                        
                        latest_signal = {
                            'timestamp': signals_df.index[i],
                            'bbw_value': bbw_val,
                            'lowest_contraction': lowest_val,
                            'difference': difference,
                            'squeeze_strength': squeeze_strength,
                            'signal_type': 'NEW_SQUEEZE'
                        }
                        
                        last_signal_contraction_level = lowest_val
                
                currently_touching = True
            else:
                # BBW moved away from contraction line
                currently_touching = False
        
        return latest_signal
    
    def _calculate_squeeze_strength(self, bbw_value, lowest_contraction):
        """Calculate squeeze strength based on BBW proximity to contraction"""
        difference_pct = abs(bbw_value - lowest_contraction) / lowest_contraction * 100
        
        if difference_pct <= 0.005:  # Very close
            return "EXTREME"
        elif difference_pct <= 0.01:  # Close
            return "HIGH"
        else:
            return "MODERATE"
