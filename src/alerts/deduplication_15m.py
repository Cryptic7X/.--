#!/usr/bin/env python3
"""
Enhanced 15-Minute BBW Signal Deduplication
Fast freshness checking with comprehensive tracking
"""

import json
import os
from datetime import datetime, timedelta

class BBW15mDeduplicator:
    def __init__(self, freshness_minutes=20):  # Increased from 15 to 20
        self.freshness_window = timedelta(minutes=freshness_minutes)
        self.cache_file = os.path.join(
            os.path.dirname(__file__), '..', '..', 'cache', 'bbw_alerts_15m.json'
        )
        self.signal_cache = self.load_cache()
        self.processing_stats = {
            'fresh_signals': 0,
            'stale_signals': 0,
            'duplicate_signals': 0,
            'cache_errors': 0
        }
    
    def load_cache(self):
        """Load 15m BBW signal cache with error handling"""
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
                print(f"📁 Loaded 15m BBW cache: {len(cache)} entries")
                return cache
        except FileNotFoundError:
            print("📁 Starting fresh 15m BBW cache")
            return {}
        except json.JSONDecodeError as e:
            print(f"⚠️ Cache file corrupted, starting fresh: {e}")
            return {}
        except Exception as e:
            print(f"❌ Cache load error: {e}")
            return {}
    
    def save_cache(self):
        """Save 15m signal cache with error handling"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.signal_cache, f, indent=2, default=str)
        except Exception as e:
            print(f"❌ Cache save error: {e}")
            self.processing_stats['cache_errors'] += 1
    
    def is_signal_fresh_and_new(self, symbol, signal_timestamp):
        """
        Enhanced freshness and deduplication checking
        
        Args:
            symbol: Coin symbol
            signal_timestamp: UTC timestamp of signal
            
        Returns:
            bool: True if alert should be sent
        """
        current_time = datetime.utcnow()
        
        # Convert timestamp if needed
        if isinstance(signal_timestamp, str):
            try:
                signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
            except ValueError:
                print(f"⚠️ Invalid timestamp format for {symbol}")
                return False
        
        # Check freshness with enhanced logging
        time_since_signal = current_time - signal_timestamp
        is_fresh = time_since_signal <= self.freshness_window
        
        if not is_fresh:
            self.processing_stats['stale_signals'] += 1
            print(f"❌ {symbol} 15m: STALE ({time_since_signal.total_seconds():.0f}s old)")
            return False
        
        # Enhanced deduplication key with minute precision
        signal_key = f"{symbol}_15M_SQUEEZE_{signal_timestamp.strftime('%Y%m%d_%H%M')}"
        
        if signal_key in self.signal_cache:
            self.processing_stats['duplicate_signals'] += 1
            print(f"❌ {symbol} 15m: DUPLICATE (already alerted)")
            return False
        
        # Record new fresh signal
        self.signal_cache[signal_key] = {
            'alerted_at': current_time.isoformat(),
            'signal_time': signal_timestamp.isoformat(),
            'freshness_seconds': time_since_signal.total_seconds(),
            'timeframe': '15m'
        }
        
        self.save_cache()
        self.processing_stats['fresh_signals'] += 1
        print(f"✅ {symbol} 15m: FRESH BBW squeeze ({time_since_signal.total_seconds():.0f}s ago)")
        return True
    
    def cleanup_old_signals(self):
        """Enhanced cleanup with statistics"""
        cutoff_time = datetime.utcnow() - timedelta(hours=4)  # Increased from 2 to 4 hours
        old_keys = []
        
        for key, data in self.signal_cache.items():
            try:
                alerted_at = datetime.fromisoformat(data['alerted_at'])
                if alerted_at < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)  # Remove invalid entries
        
        for key in old_keys:
            del self.signal_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old 15m signals")
    
    def get_processing_stats(self):
        """Return processing statistics"""
        return self.processing_stats.copy()
    
    def reset_stats(self):
        """Reset processing statistics"""
        self.processing_stats = {
            'fresh_signals': 0,
            'stale_signals': 0, 
            'duplicate_signals': 0,
            'cache_errors': 0
        }
