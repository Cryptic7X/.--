#!/usr/bin/env python3
"""
Performance monitoring for BBW Alert System
"""
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

class PerformanceMonitor:
    """Monitor and log system performance metrics"""
    
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'system': 'BBW-15m-Alert-System',
            'version': '1.0.0',
            'runs': []
        }
        
    def log_analysis_run(self, coins_analyzed: int, alerts_sent: int, execution_time: float):
        """Log analysis run metrics"""
        run_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'coins_analyzed': coins_analyzed,
            'alerts_sent': alerts_sent,
            'execution_time_seconds': round(execution_time, 2),
            'coins_per_second': round(coins_analyzed / execution_time if execution_time > 0 else 0, 2),
            'success_rate': round((coins_analyzed / coins_analyzed * 100) if coins_analyzed > 0 else 0, 2)
        }
        
        self.metrics['runs'].append(run_data)
        self._save_metrics()
        
    def _save_metrics(self):
        """Save metrics to file"""
        try:
            cache_dir = Path(__file__).parent.parent.parent / 'cache'
            cache_dir.mkdir(exist_ok=True)
            
            metrics_file = cache_dir / 'performance_metrics.json'
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
                
        except Exception as e:
            print(f"Warning: Could not save performance metrics: {e}")
