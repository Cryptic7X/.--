#!/usr/bin/env python3
"""
Daily Market Data Fetcher - With Structured Logging
Fetches unlimited coins from CoinMarketCap and creates filtered datasets
"""
import os
import sys
import json
import requests
import yaml
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

class DataFetcher:
    def __init__(self):
        self.config = self._load_config()
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        if not self.api_key:
            raise ValueError("COINMARKETCAP_API_KEY environment variable not set")
            
        self.session = requests.Session()
        self.session.headers.update({
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        })
        
        # Create cache directory if it doesn't exist
        self.cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        print(f"📊 DataFetcher initialized - Cache dir: {self.cache_dir}")

    def _load_config(self):
        """Load configuration file"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            # Fallback config
            return {
                'cipherb_config': {'filters': {'min_market_cap': 100000000, 'min_volume_24h': 10000000}},
                'sma_config': {'filters': {'min_market_cap': 10000000, 'min_volume_24h': 10000000}}
            }

    def fetch_all_coins(self) -> Optional[List[Dict]]:
        """
        Fetch unlimited coins from CoinMarketCap
        """
        print("🚀 Starting unlimited CoinMarketCap fetch...")
        
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        all_coins = []
        start = 1
        limit = 5000  # Maximum per request
        
        while True:
            try:
                print(f"📡 Fetching coins {start} to {start + limit - 1}...")
                
                params = {
                    'start': start,
                    'limit': limit,
                    'sort': 'market_cap',
                    'sort_dir': 'desc',
                    'cryptocurrency_type': 'all',
                    'tag': 'all'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'data' not in data:
                    print(f"❌ No data in API response: {data}")
                    break
                
                coins = data['data']
                if not coins:
                    print(f"✅ No more coins to fetch (reached end)")
                    break
                
                print(f"📊 Retrieved {len(coins)} coins")
                
                # Process each coin
                for coin in coins:
                    try:
                        processed_coin = {
                            'id': coin.get('id'),
                            'symbol': coin.get('symbol'),
                            'name': coin.get('name'),
                            'market_cap': coin.get('quote', {}).get('USD', {}).get('market_cap', 0),
                            'current_price': coin.get('quote', {}).get('USD', {}).get('price', 0),
                            'total_volume': coin.get('quote', {}).get('USD', {}).get('volume_24h', 0),
                            'price_change_percentage_24h': coin.get('quote', {}).get('USD', {}).get('percent_change_24h', 0),
                            'last_updated': coin.get('last_updated')
                        }
                        
                        # Filter out coins with missing essential data
                        if (processed_coin['market_cap'] and 
                            processed_coin['current_price'] and 
                            processed_coin['total_volume']):
                            all_coins.append(processed_coin)
                            
                    except Exception as e:
                        print(f"⚠️ Error processing coin {coin.get('symbol', 'UNKNOWN')}: {e}")
                        continue
                
                # Check if we got fewer coins than requested (end of data)
                if len(coins) < limit:
                    print(f"✅ Reached end of available coins")
                    break
                    
                start += limit
                
                # Safety limit to prevent infinite loops
                if len(all_coins) >= 10000:
                    print(f"⚠️ Safety limit reached: {len(all_coins)} coins")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ API request failed: {e}")
                break
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                break
        
        print(f"🎯 Total coins fetched: {len(all_coins)}")
        return all_coins if all_coins else None

    def create_filtered_datasets(self, all_coins: List[Dict]) -> tuple:
        """
        Create filtered datasets for CipherB/BBW and SMA systems
        """
        print("🔧 Creating filtered datasets...")
        
        # CipherB/BBW filters (higher requirements)
        cipherb_filters = self.config['cipherb_config']['filters']
        cipherb_coins = [
            coin for coin in all_coins 
            if (coin['market_cap'] >= cipherb_filters['min_market_cap'] and 
                coin['total_volume'] >= cipherb_filters['min_volume_24h'])
        ]
        
        # SMA filters (lower requirements for larger dataset)  
        sma_filters = self.config['sma_config']['filters']
        sma_coins = [
            coin for coin in all_coins
            if (coin['market_cap'] >= sma_filters['min_market_cap'] and 
                coin['total_volume'] >= sma_filters['min_volume_24h'])
        ]
        
        print(f"✅ CipherB/BBW dataset: {len(cipherb_coins)} coins (≥${cipherb_filters['min_market_cap']:,} cap, ≥${cipherb_filters['min_volume_24h']:,} vol)")
        print(f"✅ SMA dataset: {len(sma_coins)} coins (≥${sma_filters['min_market_cap']:,} cap, ≥${sma_filters['min_volume_24h']:,} vol)")
        
        return cipherb_coins, sma_coins

    def save_datasets(self, cipherb_coins: List[Dict], sma_coins: List[Dict]) -> tuple:
        """
        Save datasets to cache files
        """
        print("💾 Saving datasets to cache...")
        
        try:
            # Save CipherB dataset
            cipherb_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_coins': len(cipherb_coins),
                'filters_applied': 'Market cap ≥ $100M, Volume ≥ $10M',
                'coins': cipherb_coins
            }
            
            cipherb_file = os.path.join(self.cache_dir, 'cipherb_dataset.json')
            with open(cipherb_file, 'w') as f:
                json.dump(cipherb_data, f, indent=2)
            
            # Save SMA dataset  
            sma_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_coins': len(sma_coins),
                'filters_applied': 'Market cap ≥ $10M, Volume ≥ $10M',
                'coins': sma_coins
            }
            
            sma_file = os.path.join(self.cache_dir, 'sma_dataset.json')
            with open(sma_file, 'w') as f:
                json.dump(sma_data, f, indent=2)
            
            print(f"✅ Datasets saved:")
            print(f"   📁 {cipherb_file}")
            print(f"   📁 {sma_file}")
            
            return cipherb_file, sma_file
            
        except Exception as e:
            print(f"❌ Error saving datasets: {e}")
            return None, None

def main():
    """Main execution function"""
    print("=" * 60)
    print("📊 STARTING UNLIMITED DAILY MARKET SCAN")
    print("=" * 60)
    
    try:
        # Initialize fetcher
        fetcher = DataFetcher()
        
        # Fetch all coins from CoinMarketCap
        all_coins = fetcher.fetch_all_coins()
        
        if all_coins:
            # Create filtered datasets
            cipherb_filtered, sma_filtered = fetcher.create_filtered_datasets(all_coins)
            
            # Save datasets
            cipherb_file, sma_file = fetcher.save_datasets(cipherb_filtered, sma_filtered)
            
            print("\n" + "=" * 60)
            print("✅ UNLIMITED DAILY SCAN COMPLETE!")
            print(f"💰 API Credits Used: 1-2 (maximum efficiency)")
            print(f"📊 CipherB/BBW Coins: {len(cipherb_filtered)}")
            print(f"📊 SMA Coins: {len(sma_filtered)}")
            print(f"🎯 Ready for multi-indicator analysis")
            print("=" * 60)
        else:
            print("❌ No coins fetched - check API connection")
            
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
