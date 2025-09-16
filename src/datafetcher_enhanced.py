#!/usr/bin/env python3
"""
Enhanced Market Data Fetcher
12-hour refresh cycle for BBW system
"""

import os
import json
import requests
import time
from datetime import datetime, timedelta

class EnhancedDataFetcher:
    def __init__(self):
        self.session = self.create_robust_session()
        self.base_url = "https://api.coingecko.com/api/v3"
        
    def create_robust_session(self):
        """Create requests session with API key authentication"""
        session = requests.Session()
        
        # Configure retry strategy
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers with API key authentication
        session.headers.update({
            'User-Agent': 'BBW-30m-System/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Add CoinGecko Demo API key
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            session.headers['x-cg-demo-api-key'] = api_key
            print(f"🔑 Using CoinGecko Demo API Key")
            print(f"   API Key: {api_key[:8]}...{api_key[-4:]}")
        else:
            print("⚠️  No CoinGecko API Key found - using public limits")
            print("   Set COINGECKO_API_KEY environment variable")
            
        return session
    
    def fetch_market_coins(self):
        """Fetch coins using Demo API key for higher limits"""
        coins = []
        
        print("🚀 Starting CoinGecko API fetch with authentication...")
        
        # Fetch 4 pages × 250 coins = 1000 total coins
        pages = 4
        per_page = 250
        
        print(f"🎯 Target: {pages} pages × {per_page} coins = {pages * per_page} total coins")
        
        for page in range(1, pages + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'price_change_percentage': '24h'
            }
            
            print(f"📡 Fetching page {page}/{pages} (requesting {per_page} coins)")
            
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    url = f"{self.base_url}/coins/markets"
                    response = self.session.get(url, params=params, timeout=60)
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', '60')
                        wait_time = int(retry_after) + 1
                        print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                        
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        print(f"⚠️  No data for page {page} - stopping")
                        break
                        
                    coins.extend(data)
                    print(f"✅ Page {page}: {len(data)} coins fetched")
                    break
                    
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {str(e)[:100]}...")
                    if attempt == max_attempts - 1:
                        print(f"❌ All attempts failed for page {page}")
                        break
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            # Rate limiting between pages
            if page < pages:
                rate_limit_delay = 2  # 2 seconds between pages
                print(f"⏱️  Waiting {rate_limit_delay}s before next page...")
                time.sleep(rate_limit_delay)
        
        print(f"📊 Total coins fetched: {len(coins)}")
        return coins
    
    def filter_high_quality_coins(self, coins):
        """Filter coins for BBW analysis"""
        print("🔍 Filtering coins for BBW analysis...")
        
        filtered_coins = []
        for coin in coins:
            try:
                market_cap = coin.get('market_cap', 0) or 0
                volume_24h = coin.get('total_volume', 0) or 0
                
                # Apply filtering criteria
                if market_cap >= 50_000_000 and volume_24h >= 10_000_000:  # $50M & $10M
                    filtered_coins.append(coin)
                    
            except Exception as e:
                continue
        
        print(f"✅ Filtered to {len(filtered_coins)} high-quality coins for BBW analysis")
        return filtered_coins
    
    def save_market_data(self, coins):
        """Save market data to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'market_data_12h.json')
        
        data = {
            'fetched_at': datetime.utcnow().isoformat(),
            'fetched_at_ist': (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat(),
            'total_coins': len(coins),
            'coins': coins
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Market data saved to: {cache_file}")
        print(f"   Total coins: {len(coins)}")
        
        return cache_file
    
    def run_data_fetch(self):
        """Run complete data fetch cycle"""
        print("🚀 ENHANCED MARKET DATA FETCHER - 12H CYCLE")
        print("=" * 60)
        
        ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
        print(f"📅 Fetch Time: {ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"🔄 Refresh Cycle: Every 12 hours")
        print(f"🎯 Target: High-quality coins for BBW analysis")
        
        # Fetch market data
        coins = self.fetch_market_coins()
        
        if not coins:
            print("❌ No coins fetched")
            return False
        
        # Filter for quality
        filtered_coins = self.filter_high_quality_coins(coins)
        
        # Save to cache
        cache_file = self.save_market_data(filtered_coins)
        
        print("\n" + "=" * 60)
        print("✅ MARKET DATA FETCH COMPLETE")
        print("=" * 60)
        print(f"📊 Total fetched: {len(coins)}")
        print(f"🔍 After filtering: {len(filtered_coins)}")
        print(f"💾 Saved to: {cache_file}")
        print(f"⏰ Next refresh: 12 hours")
        
        return True

if __name__ == "__main__":
    fetcher = EnhancedDataFetcher()
    fetcher.run_data_fetch()
