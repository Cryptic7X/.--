"""
Fresh Alert Deduplication System for 1H BBW Analysis
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path

def get_cache_file_path():
    """Get path to alert cache file"""
    cache_dir = Path(__file__).parent.parent.parent / 'cache'
    cache_dir.mkdir(exist_ok=True)
    return cache_dir / 'fresh_alerts_1h.json'  # Changed to 1h

def load_alert_cache():
    """Load alert cache from file"""
    cache_file = get_cache_file_path()
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as file:
                return json.load(file)
        except Exception as e:
            print(f"⚠️ Error loading cache: {e}")
            return {}
    
    return {}

def save_alert_cache(cache):
    """Save alert cache to file"""
    cache_file = get_cache_file_path()
    
    try:
        with open(cache_file, 'w') as file:
            json.dump(cache, file, indent=2)
    except Exception as e:
        print(f"❌ Error saving cache: {e}")

def get_cache_key(symbol, timeframe='1h'):
    """Generate cache key for BBW squeeze alert"""
    # Use current hour for 1h timeframe caching
    current_hour = datetime.now().strftime('%Y-%m-%d-%H')
    return f"{symbol}_BBW_SQUEEZE_{timeframe}_{current_hour}"

def is_duplicate_alert(cache, cache_key, signal_timestamp):
    """
    Check if alert is duplicate based on advanced logic:
    - No duplicate for same symbol in current hour
    - Allow new alert if BBW moved away and came back
    """
    if cache_key not in cache:
        return False
    
    cached_alert = cache[cache_key]
    
    # Check timestamp freshness (1 hour window)
    try:
        cached_time = datetime.fromisoformat(cached_alert['timestamp'])
        signal_time = datetime.fromisoformat(signal_timestamp)
        
        # If more than 1 hour passed, allow new alert
        if signal_time - cached_time > timedelta(hours=1):
            return False
            
        # Within same hour - consider duplicate
        return True
        
    except Exception as e:
        print(f"⚠️ Error checking duplicate: {e}")
        return False

def cleanup_old_alerts():
    """Remove alerts older than 24 hours"""
    cache = load_alert_cache()
    
    if not cache:
        return
    
    cutoff_time = datetime.now() - timedelta(hours=24)
    cleaned_cache = {}
    
    for key, alert_data in cache.items():
        try:
            alert_time = datetime.fromisoformat(alert_data['timestamp'])
            if alert_time > cutoff_time:
                cleaned_cache[key] = alert_data
        except Exception:
            continue
    
    if len(cleaned_cache) != len(cache):
        save_alert_cache(cleaned_cache)
        print(f"🧹 Cleaned {len(cache) - len(cleaned_cache)} old alerts from cache")
