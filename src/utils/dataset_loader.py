"""
Shared Dataset Loader - Memory Optimized
"""
import json
import os
from typing import Dict, List, Optional
from src.utils.logger import trading_logger

class DatasetLoader:
    _cache = {}
    
    def __init__(self):
        self.logger = trading_logger.get_logger('DatasetLoader')
    
    def load_dataset(self, cache_file: str, filters: Dict) -> List[Dict]:
        """
        Load and filter dataset with memory optimization
        """
        cache_key = cache_file
        
        # Check if already cached
        if cache_key not in self._cache:
            self.logger.info(f"Loading dataset from {cache_file}")
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    self._cache[cache_key] = data.get('coins', [])
                    self.logger.info(f"Cached {len(self._cache[cache_key])} coins from {cache_file}")
            except FileNotFoundError:
                self.logger.error(f"Dataset not found: {cache_file}")
                return []
            except Exception as e:
                self.logger.error(f"Error loading dataset {cache_file}: {str(e)}")
                return []
        
        # Apply filters
        coins = self._cache[cache_key]
        filtered = self._apply_filters(coins, filters)
        
        self.logger.info(f"Filtered to {len(filtered)} coins (≥${filters['min_market_cap']:,} cap, ≥${filters['min_volume_24h']:,} vol)")
        return filtered
    
    def _apply_filters(self, coins: List[Dict], filters: Dict) -> List[Dict]:
        """Apply market cap and volume filters"""
        return [
            coin for coin in coins
            if coin.get('market_cap', 0) >= filters['min_market_cap'] and 
               coin.get('total_volume', 0) >= filters['min_volume_24h']
        ]
    
    def clear_cache(self):
        """Clear cached datasets"""
        self._cache.clear()
        self.logger.info("Dataset cache cleared")

# Global instance
dataset_loader = DatasetLoader()
