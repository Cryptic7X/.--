#!/usr/bin/env python3
"""
15-Minute BBW Signal Deduplication
Fast freshness checking optimized for 15m alerts
"""

import json
import os
from datetime import datetime, timedelta

class BBW15mDeduplicator:
    def __init__(self):
        self.freshness_window = timedelta(minutes=15)
        self.cache_file = os.path.join(
            os.path.dirname(__file__), '..', '..', 'cache', 'bbw_alerts_15m.json'
        )
        self.signal_cache = self.load_cache()
    
    def load_cache(self):
        """Load 15m BBW signal cache"""
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
                print(f"📁 Loaded 15m BBW cache: {len(cache)} entries")
                return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh 15m BBW cache")
            return {}
    
    def save_cache(self):
        """Save 15m signal cache"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.signal_cache, f, indent=2, default=str)
    
    def is_signal_fresh_and_new(self, symbol, signal_timestamp):
        """
        Check if 15m BBW squeeze is fresh and new
        
        Args:
            symbol: Coin symbol
            signal_timestamp: UTC timestamp of signal
            
        Returns:
            bool: True if alert should be sent
        """
        current_time = datetime.utcnow()
        
        # Convert timestamp if needed
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        # Check 15m freshness
        time_since_signal = current_time - signal_timestamp
        is_fresh = time_since_signal <= self.freshness_window
        
        if not is_fresh:
            print(f"❌ {symbol} 15m: STALE ({time_since_signal.total_seconds():.0f}s old)")
            return False
        
        # Check for duplicates
        signal_key = f"{symbol}_15M_SQUEEZE_{signal_timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        if signal_key in self.signal_cache:
            print(f"❌ {symbol} 15m: DUPLICATE (already alerted)")
            return False
        
        # Record new signal
        self.signal_cache[signal_key] = {
            'alerted_at': current_time.isoformat(),
            'signal_time': signal_timestamp.isoformat(),
            'freshness_seconds': time_since_signal.total_seconds(),
            'timeframe': '15m'
        }
        
        self.save_cache()
        print(f"✅ {symbol} 15m: FRESH BBW squeeze ({time_since_signal.total_seconds():.0f}s ago)")
        return True
    
    def cleanup_old_signals(self):
        """Remove signals older than 2 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=2)
        old_keys = []
        
        for key, data in self.signal_cache.items():
            try:
                alerted_at = datetime.fromisoformat(data['alerted_at'])
                if alerted_at < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)
        
        for key in old_keys:
            del self.signal_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old 15m signals")
