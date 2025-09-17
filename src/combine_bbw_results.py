#!/usr/bin/env python3
"""
BBW Results Combiner - Combine All Matrix Job Results
Collects signals from all batches and sends consolidated alert
"""

import os
import json
import glob
from datetime import datetime, timedelta

import sys
sys.path.insert(0, os.path.dirname(__file__))

from alerts.telegram_coinglass import TelegramCoinGlassAlert

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

class BBWResultsCombiner:
    def __init__(self):
        self.telegram = TelegramCoinGlassAlert()
        
    def collect_all_results(self):
        """Collect results from all batch jobs"""
        all_signals = []
        batch_stats = []
        
        # Look for batch result files
        artifacts_dir = os.path.join(os.path.dirname(__file__), '..', 'artifacts')
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        
        # Check both locations for results
        search_patterns = [
            os.path.join(artifacts_dir, '*/bbw_batch_*.json'),
            os.path.join(cache_dir, 'bbw_batch_*.json')
        ]
        
        result_files = []
        for pattern in search_patterns:
            result_files.extend(glob.glob(pattern))
        
        print(f"🔍 Found {len(result_files)} batch result files")
        
        for result_file in result_files:
            try:
                with open(result_file, 'r') as f:
                    batch_data = json.load(f)
                
                batch_index = batch_data.get('batch_index', 0)
                batch_signals = batch_data.get('signals', [])
                batch_size = batch_data.get('batch_size', 0)
                
                all_signals.extend(batch_signals)
                batch_stats.append({
                    'batch_index': batch_index,
                    'coins_processed': batch_size,
                    'signals_found': len(batch_signals)
                })
                
                print(f"📊 Batch {batch_index + 1}: {batch_size} coins → {len(batch_signals)} signals")
                
            except Exception as e:
                print(f"❌ Error reading {result_file}: {e}")
        
        return all_signals, batch_stats
    
    def run_combination(self):
        """Combine all results and send consolidated alert"""
        ist_current = get_ist_time()
        
        print("=" * 80)
        print("🔗 BBW RESULTS COMBINATION - ALL BATCHES")
        print("=" * 80)
        print(f"📅 Combination Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S')} IST")
        
        # Collect all results
        all_signals, batch_stats = self.collect_all_results()
        
        # Summary statistics
        total_coins_processed = sum(batch['coins_processed'] for batch in batch_stats)
        total_batches = len(batch_stats)
        
        print(f"\n📊 COMBINATION SUMMARY:")
        print(f"   Total batches processed: {total_batches}")
        print(f"   Total coins analyzed: {total_coins_processed}")
        print(f"   Total BBW signals: {len(all_signals)}")
        
        if batch_stats:
            print(f"\n📋 Batch Details:")
            for batch in sorted(batch_stats, key=lambda x: x['batch_index']):
                print(f"   Batch {batch['batch_index'] + 1}: {batch['coins_processed']} coins → {batch['signals_found']} signals")
        
        # Send consolidated alert if signals found
        if all_signals:
            print(f"\n📱 Sending consolidated alert with {len(all_signals)} signals...")
            
            try:
                success = self.telegram.send_consolidated_bbw_alert(all_signals)
                if success:
                    print(f"✅ CONSOLIDATED ALERT SENT: {len(all_signals)} BBW signals")
                else:
                    print(f"❌ Alert sending failed")
            except Exception as e:
                print(f"❌ Alert error: {str(e)[:100]}")
        else:
            print(f"\n📭 No BBW signals found across all batches")
            print(f"   Possible reasons:")
            print(f"   - Market not in squeeze phase currently")
            print(f"   - All signals filtered as stale/duplicate")
            print(f"   - Tolerance settings too strict")
        
        print("\n" + "=" * 80)
        print("✅ BBW COMBINATION COMPLETE")
        print(f"🎯 TOTAL ANALYSIS: {total_coins_processed} coins across {total_batches} parallel jobs")
        print(f"🚨 FINAL SIGNALS: {len(all_signals)} BBW squeeze events detected")
        print("=" * 80)

if __name__ == "__main__":
    combiner = BBWResultsCombiner()
    combiner.run_combination()
