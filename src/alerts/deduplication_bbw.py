#!/usr/bin/env python3
"""
BBW Multi-Timeframe Signal Deduplication System

Manages separate cache files for 30m, 1h, and aligned signals
with different freshness windows per timeframe.
"""

import json
import os
from datetime import datetime, timedelta

class BBWSignalDeduplicator:
    def __init__(self):
        # Cache files for different signal types
        self.cache_files = {
            '30m': os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'bbw_alerts_30m.json'),
            '1h': os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'bbw_alerts_1h.json'),
            'aligned': os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'bbw_alerts_aligned.json')
        }
        
        # Freshness windows per timeframe
        self.freshness_windows = {
            '30m': timedelta(minutes=35),
            '1h': timedelta(minutes=65)
        }
        
        # Load existing caches
        self.signal_caches = {}
        for signal_type, cache_file in self.cache_files.items():
            self.signal_caches[signal_type] = self.load_cache(cache_file, signal_type)
    
    def load_cache(self, cache_file, signal_type):
        """Load signal cache from file"""
        try:
            with open(cache_file, 'r') as f:
                cache = json.load(f)
                print(f"📁 Loaded BBW {signal_type} cache: {len(cache)} entries")
                return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"📁 Starting fresh BBW {signal_type} cache")
            return {}
    
    def save_cache(self, signal_type):
        """Save signal cache to file"""
        cache_file = self.cache_files[signal_type]
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        
        with open(cache_file, 'w') as f:
            json.dump(self.signal_caches[signal_type], f, indent=2, default=str)
    
    def is_signal_fresh_and_new(self, symbol, timeframe, signal_timestamp):
        """
        Check if BBW squeeze signal is fresh and new for specific timeframe
        
        Args:
            symbol: Coin symbol (e.g., 'ETH')
            timeframe: '30m' or '1h' 
            signal_timestamp: UTC timestamp of signal
            
        Returns:
            bool: True if signal should be sent
        """
        current_time = datetime.utcnow()
        
        # Convert timestamp if needed
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        # Check freshness based on timeframe
        time_since_signal = current_time - signal_timestamp
        freshness_window = self.freshness_windows[timeframe]
        
        is_fresh = time_since_signal <= freshness_window
        
        if not is_fresh:
            print(f"❌ {symbol} {timeframe}: STALE signal ({time_since_signal.total_seconds():.0f}s old)")
            return False
        
        # Check if already alerted
        signal_key = f"{symbol}_SQUEEZE_{signal_timestamp.strftime('%Y%m%d_%H%M%S')}"
        cache = self.signal_caches[timeframe]
        
        if signal_key in cache:
            print(f"❌ {symbol} {timeframe}: DUPLICATE signal (already alerted)")
            return False
        
        # Record new signal
        cache[signal_key] = {
            'alerted_at': current_time.isoformat(),
            'signal_time': signal_timestamp.isoformat(),
            'freshness_seconds': time_since_signal.total_seconds(),
            'timeframe': timeframe
        }
        
        self.save_cache(timeframe)
        print(f"✅ {symbol} {timeframe}: FRESH BBW squeeze ({time_since_signal.total_seconds():.0f}s ago)")
        return True
    
    def is_aligned_signal_new(self, symbol, signal_30m_timestamp, signal_1h_timestamp):
        """
        Check if aligned (both timeframes) signal is new
        
        Returns:
            bool: True if aligned signal should be sent
        """
        current_time = datetime.utcnow()
        
        # Create unique key for aligned signal
        signal_key = f"{symbol}_ALIGNED_{signal_30m_timestamp.strftime('%Y%m%d_%H%M')}_{signal_1h_timestamp.strftime('%Y%m%d_%H%M')}"
        
        cache = self.signal_caches['aligned']
        
        if signal_key in cache:
            print(f"❌ {symbol} ALIGNED: DUPLICATE signal (already alerted)")
            return False
        
        # Record new aligned signal
        cache[signal_key] = {
            'alerted_at': current_time.isoformat(),
            'signal_30m_time': signal_30m_timestamp.isoformat(),
            'signal_1h_time': signal_1h_timestamp.isoformat(),
            'timeframe': 'aligned'
        }
        
        self.save_cache('aligned')
        print(f"✅ {symbol} ALIGNED: NEW both-timeframe squeeze signal")
        return True
    
    def cleanup_old_signals(self):
        """Remove signal records older than 4 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=4)
        total_cleaned = 0
        
        for signal_type in self.signal_caches:
            cache = self.signal_caches[signal_type]
            old_keys = []
            
            for key, data in cache.items():
                try:
                    alerted_at = datetime.fromisoformat(data['alerted_at'])
                    if alerted_at < cutoff_time:
                        old_keys.append(key)
                except (ValueError, TypeError, KeyError):
                    old_keys.append(key)  # Remove invalid entries
            
            for key in old_keys:
                del cache[key]
            
            if old_keys:
                self.save_cache(signal_type)
                total_cleaned += len(old_keys)
        
        if total_cleaned > 0:
            print(f"🧹 Cleaned {total_cleaned} old BBW signal records")
