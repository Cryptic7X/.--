#!/usr/bin/env python3
"""
BBW Calculator - Exact Pine Script Replication
//@version=6 indicator logic implemented in Python
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class BBWCalculator:
    """
    Exact replication of Pine Script BBW indicator:
    
    length = input.int(20, minval = 1)
    src = input(close, title = "Source")
    mult = input.float(2.0, minval = 0.001, maxval =50, title ="StdDev")
    
    expansionLengthInput = input.int(125, "Highest Expansion Length")
    contractionLengthInput = input.int(125, "Lowest Contraction Length")
    
    basis = ta.sma(src, length)
    dev = mult * ta.stdev(src, length)
    upper = basis + dev
    lower = basis - dev
    bbw = ((upper - lower) / basis) * 100
    
    plot(bbw, "Bollinger BandWidth", color = #2962FF)
    plot(ta.highest(bbw, expansionLengthInput), "Highest Expansion", color = color.new(#F23645, 50))
    plot(ta.lowest(bbw, contractionLengthInput), "Lowest Contraction", color = color.new(#089981, 50))
    """
    
    def __init__(self, length: int = 20, mult: float = 2.0, contraction_length: int = 125):
        self.length = length
        self.mult = mult
        self.contraction_length = contraction_length
    
    def calculate_bbw_exact(self, ohlcv_data: pd.DataFrame) -> Dict:
        """
        Calculate BBW exactly as Pine Script does
        
        Args:
            ohlcv_data: DataFrame with OHLCV data
            
        Returns:
            Dict with BBW calculations
        """
        try:
            if len(ohlcv_data) < self.contraction_length:
                raise ValueError(f"Insufficient data: {len(ohlcv_data)} < {self.contraction_length}")
            
            close_prices = ohlcv_data['close']
            
            # Step 1: Calculate Bollinger Bands (exact Pine Script logic)
            # basis = ta.sma(src, length)
            basis = close_prices.rolling(window=self.length, min_periods=self.length).mean()
            
            # dev = mult * ta.stdev(src, length)
            # Note: Pine Script ta.stdev uses sample standard deviation (ddof=1)
            std_dev = close_prices.rolling(window=self.length, min_periods=self.length).std(ddof=1)
            dev = self.mult * std_dev
            
            # upper = basis + dev, lower = basis - dev
            upper = basis + dev
            lower = basis - dev
            
            # Step 2: Calculate BBW
            # bbw = ((upper - lower) / basis) * 100
            bbw = ((upper - lower) / basis) * 100
            
            # Step 3: Calculate rolling highest/lowest (exact Pine Script logic)
            # ta.highest(bbw, expansionLengthInput)
            highest_expansion = bbw.rolling(window=self.contraction_length, min_periods=self.contraction_length).max()
            
            # ta.lowest(bbw, contractionLengthInput)
            lowest_contraction = bbw.rolling(window=self.contraction_length, min_periods=self.contraction_length).min()
            
            # Remove NaN values and ensure we have valid data
            valid_data = ~(bbw.isna() | highest_expansion.isna() | lowest_contraction.isna())
            
            if not valid_data.any():
                raise ValueError("No valid BBW data after calculation")
            
            result = {
                'bbw': bbw[valid_data],
                'highest_expansion': highest_expansion[valid_data],
                'lowest_contraction': lowest_contraction[valid_data],
                'basis': basis[valid_data],
                'upper': upper[valid_data],
                'lower': lower[valid_data],
                'timestamps': ohlcv_data.index[valid_data]
            }
            
            logger.debug(f"✅ BBW calculated: {len(result['bbw'])} valid points")
            return result
            
        except Exception as e:
            logger.error(f"❌ BBW calculation error: {e}")
            raise
    
    def detect_squeeze(self, bbw_data: Dict, tolerance: float = 0.001) -> Dict:
        """
        Detect BBW squeeze - when BBW touches lowest contraction
        
        Args:
            bbw_data: Output from calculate_bbw_exact()
            tolerance: Tolerance for "touching" detection
            
        Returns:
            Dict with squeeze detection results
        """
        try:
            if len(bbw_data['bbw']) == 0:
                return {'is_squeeze': False, 'error': 'No BBW data'}
            
            # Get current (latest) values
            current_bbw = bbw_data['bbw'].iloc[-1]
            current_lowest = bbw_data['lowest_contraction'].iloc[-1]
            current_highest = bbw_data['highest_expansion'].iloc[-1]
            
            # Squeeze detection: BBW touching lowest contraction line
            # In Pine Script, this would be when the blue line touches the green line
            difference = abs(current_bbw - current_lowest)
            is_squeeze = difference <= tolerance
            
            # Additional context
            bbw_percentile = ((current_bbw - current_lowest) / 
                            (current_highest - current_lowest)) * 100 if current_highest > current_lowest else 0
            
            result = {
                'is_squeeze': is_squeeze,
                'bbw_value': round(current_bbw, 4),
                'lowest_contraction': round(current_lowest, 4),
                'highest_expansion': round(current_highest, 4),
                'difference': round(difference, 6),
                'bbw_percentile': round(bbw_percentile, 2),
                'squeeze_strength': round(1 / (difference + 0.001), 2),  # Higher = stronger squeeze
                'timestamp': bbw_data['timestamps'].iloc[-1]
            }
            
            if is_squeeze:
                logger.info(f"🎯 BBW Squeeze detected! BBW: {current_bbw:.4f}, LC: {current_lowest:.4f}, Diff: {difference:.6f}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Squeeze detection error: {e}")
            return {'is_squeeze': False, 'error': str(e)}
    
    def validate_pine_script_accuracy(self, bbw_data: Dict, tradingview_values: Dict) -> Dict:
        """
        Validate our calculation against TradingView Pine Script values
        
        Args:
            bbw_data: Our calculated BBW data
            tradingview_values: Values from TradingView for comparison
            
        Returns:
            Validation results
        """
        try:
            current_bbw = bbw_data['bbw'].iloc[-1]
            tv_bbw = tradingview_values.get('bbw')
            
            if tv_bbw is None:
                return {'valid': False, 'error': 'No TradingView reference value'}
            
            difference = abs(current_bbw - tv_bbw)
            percentage_error = (difference / tv_bbw) * 100 if tv_bbw != 0 else float('inf')
            
            # Consider accurate if within 0.1% difference
            is_accurate = percentage_error < 0.1
            
            return {
                'valid': True,
                'accurate': is_accurate,
                'our_bbw': round(current_bbw, 4),
                'tradingview_bbw': round(tv_bbw, 4),
                'difference': round(difference, 6),
                'percentage_error': round(percentage_error, 4)
            }
            
        except Exception as e:
            return {'valid': False, 'error': str(e)}
