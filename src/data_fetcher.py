#!/usr/bin/env python3
"""
Multi-Indicator Data Fetcher - Unlimited CoinMarketCap Integration
Fetches all coins meeting criteria for CipherB, BBW, and SMA systems
"""

import os
import json
import time
import requests
import yaml
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class MultiIndicatorDataFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.session = self.create_robust_session()
        self.blocked_coins = self.load_blocked_coins()

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)

    def load_blocked_coins(self):
        """Load blocked coins from blocked_coins.txt"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins")
                    
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
            
        return blocked_coins

    def create_robust_session(self):
        """Create session with CMC API authentication"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            'User-Agent': 'Multi-Indicator-System/3.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })

        api_key = os.getenv('COINMARKETCAP_API_KEY')
        if api_key:
            session.headers['X-CMC_PRO_API_KEY'] = api_key
            print("✅ Using CoinMarketCap API Key")
        else:
            print("❌ No CoinMarketCap API Key found")
            return None

        return session

    def fetch_unlimited_coins(self):
        """
        Fetch unlimited coins with server-side filtering for both datasets
        """
        if not self.session:
            return [], []

        print(f"🚀 Fetching unlimited coins from CoinMarketCap...")
        
        # Fetch large dataset for SMA (less restrictive)
        sma_coins = self.fetch_coins_for_dataset(
            min_market_cap=self.config['market_data']['sma_filters']['min_market_cap'],
            min_volume=self.config['market_data']['sma_filters']['min_volume_24h'],
            limit=5000,
            dataset_name="SMA"
        )
        
        # Filter SMA coins for CipherB/BBW (more restrictive)
        cipherb_coins = []
        cipherb_min_cap = self.config['market_data']['cipherb_filters']['min_market_cap']
        cipherb_min_vol = self.config['market_data']['cipherb_filters']['min_volume_24h']
        
        for coin in sma_coins:
            if (coin.get('market_cap', 0) >= cipherb_min_cap and 
                coin.get('total_volume', 0) >= cipherb_min_vol):
                cipherb_coins.append(coin)
        
        print(f"✅ Dataset Summary:")
        print(f"   CipherB/BBW: {len(cipherb_coins)} coins (≥$100M cap, ≥$10M vol)")
        print(f"   SMA: {len(sma_coins)} coins (≥$10M cap, ≥$10M vol)")
        
        return cipherb_coins, sma_coins

    def fetch_coins_for_dataset(self, min_market_cap, min_volume, limit, dataset_name):
        """Fetch coins with specific filters"""
        base_url = "https://pro-api.coinmarketcap.com/v1"
        
        params = {
            'start': 1,
            'limit': limit,
            'convert': 'USD',
            'sort': 'market_cap',
            'sort_dir': 'desc',
            'market_cap_min': min_market_cap,
            'volume_24h_min': min_volume
        }

        try:
            url = f"{base_url}/cryptocurrency/listings/latest"
            print(f"📡 Fetching {dataset_name} dataset...")
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '60')
                wait_time = int(retry_after)
                print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time + 1)
                response = self.session.get(url, params=params, timeout=30)
            
            response.raise_for_status()
            data = response.json()
            
            if 'data' not in data or not data['data']:
                print(f"📭 No data received for {dataset_name}")
                return []
            
            # Convert CMC format to our expected format
            coins = []
            for coin in data['data']:
                try:
                    coin_data = {
                        'id': coin['id'],
                        'symbol': coin['symbol'],
                        'name': coin['name'],
                        'current_price': coin['quote']['USD']['price'],
                        'market_cap': coin['quote']['USD']['market_cap'],
                        'total_volume': coin['quote']['USD']['volume_24h'],
                        'price_change_percentage_24h': coin['quote']['USD']['percent_change_24h'],
                        'market_cap_rank': coin['cmc_rank']
                    }
                    coins.append(coin_data)
                except (KeyError, TypeError) as e:
                    continue
            
            print(f"✅ {dataset_name}: Fetched {len(coins)} coins")
            
            return coins
            
        except Exception as e:
            print(f"❌ {dataset_name} fetch failed: {str(e)[:100]}")
            return []

    def filter_blocked_coins(self, coins_list, dataset_name):
        """Filter out blocked coins locally"""
        if not self.blocked_coins:
            return coins_list
            
        filtered_coins = []
        blocked_count = 0
        
        for coin in coins_list:
            symbol = coin.get('symbol', '').upper()
            if symbol in self.blocked_coins:
                blocked_count += 1
                continue
            filtered_coins.append(coin)
        
        print(f"🛡️ {dataset_name}: Filtered out {blocked_count} blocked coins")
        return filtered_coins

    def save_datasets(self, cipherb_coins, sma_coins):
        """Save both datasets to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Save CipherB/BBW dataset
        cipherb_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'source': 'CoinMarketCap-Unlimited',
            'dataset_type': 'CipherB-BBW',
            'filters_applied': self.config['market_data']['cipherb_filters'],
            'total_coins': len(cipherb_coins),
            'coins': cipherb_coins
        }
        
        cipherb_file = os.path.join(cache_dir, 'cipherb_dataset.json')
        with open(cipherb_file, 'w') as f:
            json.dump(cipherb_data, f, indent=2)
        
        # Save SMA dataset
        sma_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'source': 'CoinMarketCap-Unlimited',
            'dataset_type': 'SMA',
            'filters_applied': self.config['market_data']['sma_filters'],
            'total_coins': len(sma_coins),
            'coins': sma_coins
        }
        
        sma_file = os.path.join(cache_dir, 'sma_dataset.json')
        with open(sma_file, 'w') as f:
            json.dump(sma_data, f, indent=2)
        
        print(f"\n💾 Datasets saved:")
        print(f"   CipherB/BBW: {cipherb_file}")
        print(f"   SMA: {sma_file}")
        
        return cipherb_file, sma_file

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-Indicator Data Fetcher')
    parser.add_argument('--daily-scan', action='store_true', help='Run unlimited daily scan')
    args = parser.parse_args()

    if args.daily_scan:
        print("🌅 UNLIMITED MULTI-INDICATOR DAILY SCAN")
        print("="*60)
        
        fetcher = MultiIndicatorDataFetcher()
        
        # Fetch unlimited coins for both datasets
        cipherb_coins, sma_coins = fetcher.fetch_unlimited_coins()
        
        if cipherb_coins or sma_coins:
            # Filter blocked coins
            cipherb_filtered = fetcher.filter_blocked_coins(cipherb_coins, "CipherB/BBW")
            sma_filtered = fetcher.filter_blocked_coins(sma_coins, "SMA")
            
            # Save datasets
            cipherb_file, sma_file = fetcher.save_datasets(cipherb_filtered, sma_filtered)
            
            print("\n" + "="*60)
            print("✅ UNLIMITED DAILY SCAN COMPLETE!")
            print(f"💰 API Credits Used: 1-2 (maximum efficiency)")
            print(f"📊 CipherB/BBW Coins: {len(cipherb_filtered)}")
            print(f"📊 SMA Coins: {len(sma_filtered)}")
            print(f"🎯 Ready for multi-indicator analysis")
            print("="*60)
        else:
            print("❌ No coins fetched - check API connection")

if __name__ == '__main__':
    main()
