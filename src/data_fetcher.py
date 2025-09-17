#!/usr/bin/env python3

"""
CoinGecko Market Data Fetcher for BBW 1H Alert System
Fetches top 500+ coins with API authentication
"""

import os
import json
import time
import requests
import yaml
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CoinGeckoFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.session = self.create_robust_session()

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path) as f:
                return yaml.safe_load(f)
        except:
            # Return default config if file doesn't exist or has issues
            return self.get_default_config()

    def get_default_config(self):
        """Default configuration for BBW 1H system"""
        return {
            'apis': {
                'coingecko': {
                    'base_url': 'https://api.coingecko.com/api/v3',
                    'rate_limit': 2
                }
            },
            'scan': {
                'pages': 4,
                'coins_per_page': 250
            },
            'market_filter': {
                'min_market_cap': 50000000,
                'min_volume_24h': 10000000
            }
        }

    def create_robust_session(self):
        """Create requests session with API key authentication"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers with API key authentication
        session.headers.update({
            'User-Agent': 'BBW-1h-System/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Add CoinGecko Demo API key
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            session.headers['x-cg-demo-api-key'] = api_key
            print("✅ Using CoinGecko Demo API Key")
            print(f"  API Key: {api_key[:8]}...{api_key[-4:]}")
        else:
            print("⚠️ No CoinGecko API Key found - using public limits")
            print("  Set COINGECKO_API_KEY environment variable")
        
        return session

    def fetch_market_coins(self):
        """Fetch coins using Demo API key for higher limits"""
        # Use safe config access with defaults
        base_url = self.config.get('apis', {}).get('coingecko', {}).get('base_url', 'https://api.coingecko.com/api/v3')
        coins = []
        
        print(f"🚀 Starting CoinGecko API fetch for BBW 1H system...")
        
        pages = self.config.get('scan', {}).get('pages', 2)
        per_page = min(self.config.get('scan', {}).get('coins_per_page', 250), 250)
        
        print(f"📊 Target: {pages} pages × {per_page} coins = {pages * per_page} total coins")
        
        for page in range(1, pages + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'price_change_percentage': '24h'
            }
            
            print(f"\n📄 Fetching page {page}/{pages} (requesting {per_page} coins)")
            
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    url = f"{base_url}/coins/markets"
                    response = self.session.get(url, params=params, timeout=60)
                    
                    if response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', '60')
                        wait_time = int(retry_after)
                        print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time + 1)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        print(f"📭 No data for page {page} - stopping")
                        break
                    
                    coins.extend(data)
                    print(f"✅ Page {page}: {len(data)} coins fetched")
                    break
                    
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {str(e)[:100]}")
                    if attempt == max_attempts - 1:
                        print(f"❌ All attempts failed for page {page}")
                        break
                    time.sleep((2 ** attempt) + 1)
            
            # Rate limiting between pages
            if page < pages:
                rate_limit_delay = self.config.get('apis', {}).get('coingecko', {}).get('rate_limit', 2)
                print(f"⏳ Waiting {rate_limit_delay}s before next page...")
                time.sleep(rate_limit_delay)
        
        print(f"\n📊 Total coins fetched: {len(coins)}")
        return coins

    def filter_bbw_coins(self, coins):
        """Filter coins for BBW 1H analysis with updated criteria"""
        print(f"\n🔍 Filtering coins for BBW 1H analysis...")
        
        filtered_coins = []
        
        # Get filter values from config with defaults
        min_market_cap = self.config.get('market_filter', {}).get('min_market_cap', 50_000_000)
        min_volume_24h = self.config.get('market_filter', {}).get('min_volume_24h', 10_000_000)
        
        print(f"  Filters: Market Cap ≥ ${min_market_cap:,}, Volume ≥ ${min_volume_24h:,}")
        
        for coin in coins:
            try:
                market_cap = coin.get('market_cap', 0) or 0
                volume_24h = coin.get('total_volume', 0) or 0
                
                if market_cap >= min_market_cap and volume_24h >= min_volume_24h:
                    filtered_coins.append(coin)
                    
            except Exception as e:
                print(f"⚠️ Error filtering coin {coin.get('symbol', 'UNKNOWN')}: {e}")
                continue
        
        print(f"✅ Filtered to {len(filtered_coins)} high-quality coins for BBW 1H analysis")
        return filtered_coins

    def save_market_data(self, coins):
        """Save market data to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        
        data = {
            'updated_at': datetime.utcnow().isoformat(),
            'total_coins': len(coins),
            'coins': coins,
            'system': 'BBW_1H',
            'filters': {
                'min_market_cap': self.config.get('market_filter', {}).get('min_market_cap', 50_000_000),
                'min_volume_24h': self.config.get('market_filter', {}).get('min_volume_24h', 10_000_000)
            }
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Saved {len(coins)} coins to cache")
        return cache_file

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='CoinGecko Market Data Fetcher for BBW 1H System')
    parser.add_argument('--daily-scan', action='store_true', help='Run market data scan every 6 hours')
    
    args = parser.parse_args()
    
    if args.daily_scan:
        print("🌅 Starting market data scan for BBW 1H system...")
        
        fetcher = CoinGeckoFetcher()
        
        # Fetch market data
        coins = fetcher.fetch_market_coins()
        
        if coins:
            # Filter for BBW analysis
            filtered_coins = fetcher.filter_bbw_coins(coins)
            
            # Save to cache
            cache_file = fetcher.save_market_data(filtered_coins)
            
            print(f"\n✅ Market data scan complete!")
            print(f"📄 Cache file: {cache_file}")
            print(f"🎯 Ready for BBW 1H analysis")
            print(f"💡 Next analysis will use {len(filtered_coins)} coins")
        else:
            print("❌ No coins fetched - check API connection")

if __name__ == '__main__':
    main()
