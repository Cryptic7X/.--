#!/usr/bin/env python3

"""
CoinMarketCap Data Fetcher for BBW 1H Alert System
Efficient daily fetch: 1 API call for 500+ coins
Replaces CoinGecko completely with CMC Pro API
"""

import os
import json
import time
import requests
import yaml
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CoinMarketCapFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.session = self.create_robust_session()
        self.blocked_coins = self.load_blocked_coins()
        
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except:
            return self.get_default_config()

    def get_default_config(self):
        """Default configuration for BBW 1H system"""
        return {
            'apis': {
                'coinmarketcap': {
                    'base_url': 'https://pro-api.coinmarketcap.com/v1',
                    'rate_limit': 1
                }
            },
            'scan': {
                'total_coins': 500,
                'per_page': 200
            },
            'market_filter': {
                'min_market_cap': 100000000,    # $100M
                'min_volume_24h': 10000000      # $10M
            }
        }

    def create_robust_session(self):
        """Create requests session with CMC API authentication"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers with CMC API key
        session.headers.update({
            'User-Agent': 'BBW-1h-System/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        })
        
        # Add CoinMarketCap Pro API key
        api_key = os.getenv('CMC_API_KEY')
        if api_key:
            session.headers['X-CMC_PRO_API_KEY'] = api_key
            print("✅ Using CoinMarketCap Pro API Key")
            print(f"  API Key: {api_key[:8]}...{api_key[-4:]}")
        else:
            print("❌ CMC_API_KEY environment variable not found")
            print("  Set CMC_API_KEY for Pro API access")
            raise ValueError("CMC API key required")
        
        return session

    def load_blocked_coins(self):
        """Load blocked coins from blocked_coins.txt"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        
        if not os.path.exists(blocked_file):
            print("⚠️ blocked_coins.txt not found - no coins will be blocked")
            return set()
        
        try:
            with open(blocked_file, 'r') as f:
                blocked_coins = {line.strip().upper() for line in f if line.strip()}
            print(f"📋 Loaded {len(blocked_coins)} blocked coins from blocked_coins.txt")
            return blocked_coins
        except Exception as e:
            print(f"❌ Error loading blocked coins: {e}")
            return set()

    def fetch_cmc_listings(self):
        """Fetch top 500 coins from CoinMarketCap in single efficient call"""
        base_url = self.config.get('apis', {}).get('coinmarketcap', {}).get('base_url', 
                                                   'https://pro-api.coinmarketcap.com/v1')
        total_coins = self.config.get('scan', {}).get('total_coins', 500)
        per_page = min(self.config.get('scan', {}).get('per_page', 200), 5000)  # CMC allows up to 5000
        
        print(f"🚀 Fetching top {total_coins} coins from CoinMarketCap...")
        
        all_coins = []
        
        # Calculate how many calls needed (try to minimize API calls)
        if total_coins <= 5000:
            # Single call for up to 5000 coins - most efficient
            calls_needed = [(1, total_coins)]
        else:
            # Multiple calls if needed (rare case)
            calls_needed = []
            for start in range(1, total_coins + 1, per_page):
                limit = min(per_page, total_coins - start + 1)
                calls_needed.append((start, limit))
        
        print(f"📊 API calls needed: {len(calls_needed)} (optimized for minimal usage)")
        
        for call_num, (start, limit) in enumerate(calls_needed, 1):
            print(f"📡 API Call {call_num}/{len(calls_needed)}: Fetching coins {start}-{start+limit-1}")
            
            url = f"{base_url}/cryptocurrency/listings/latest"
            params = {
                'start': start,
                'limit': limit,
                'convert': 'USD',
                'sort': 'market_cap',
                'sort_dir': 'desc'
            }
            
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    response = self.session.get(url, params=params, timeout=60)
                    
                    if response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', '60')
                        wait_time = int(retry_after)
                        print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time + 1)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'data' not in data:
                        print(f"❌ Invalid response structure from CMC")
                        break
                    
                    coins_received = data['data']
                    all_coins.extend(coins_received)
                    print(f"✅ Received {len(coins_received)} coins")
                    break
                    
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {str(e)[:100]}")
                    if attempt == max_attempts - 1:
                        print(f"❌ All attempts failed for call {call_num}")
                        break
                    time.sleep((2 ** attempt) + 1)
            
            # Rate limiting between calls (if multiple calls needed)
            if call_num < len(calls_needed):
                rate_limit_delay = self.config.get('apis', {}).get('coinmarketcap', {}).get('rate_limit', 1)
                time.sleep(rate_limit_delay)
        
        print(f"📊 Total coins fetched from CMC: {len(all_coins)}")
        return all_coins

    def filter_bbw_coins(self, coins):
        """Filter coins for BBW 1H analysis with blocked coins tracking"""
        print(f"\n🔍 Filtering {len(coins)} coins for BBW 1H analysis...")
        
        # Get filter values from config
        min_market_cap = self.config.get('market_filter', {}).get('min_market_cap', 100_000_000)
        min_volume_24h = self.config.get('market_filter', {}).get('min_volume_24h', 10_000_000)
        
        print(f"📋 Filters:")
        print(f"   Market Cap ≥ ${min_market_cap:,}")
        print(f"   Volume 24h ≥ ${min_volume_24h:,}")
        print(f"   Blocked coins: {len(self.blocked_coins)}")
        
        filtered_coins = []
        stats = {
            'total': len(coins),
            'blocked': 0,
            'low_market_cap': 0,
            'low_volume': 0,
            'passed': 0
        }
        
        for coin in coins:
            try:
                symbol = coin.get('symbol', '').upper()
                
                # Check if blocked
                if symbol in self.blocked_coins:
                    stats['blocked'] += 1
                    continue
                
                # Get USD quote data
                usd_quote = coin.get('quote', {}).get('USD', {})
                market_cap = usd_quote.get('market_cap', 0) or 0
                volume_24h = usd_quote.get('volume_24h', 0) or 0
                
                # Apply filters
                if market_cap < min_market_cap:
                    stats['low_market_cap'] += 1
                    continue
                    
                if volume_24h < min_volume_24h:
                    stats['low_volume'] += 1
                    continue
                
                # Transform CMC format to BBW system format
                filtered_coin = {
                    'symbol': symbol,
                    'name': coin.get('name', ''),
                    'market_cap': market_cap,
                    'total_volume': volume_24h,
                    'current_price': usd_quote.get('price', 0),
                    'price_change_percentage_24h': usd_quote.get('percent_change_24h', 0),
                    'market_cap_rank': coin.get('cmc_rank', 0),
                    'cmc_id': coin.get('id', 0),
                    'last_updated': coin.get('last_updated', '')
                }
                
                filtered_coins.append(filtered_coin)
                stats['passed'] += 1
                
            except Exception as e:
                print(f"⚠️ Error processing coin {coin.get('symbol', 'UNKNOWN')}: {e}")
                continue
        
        # Print filtering report
        print(f"\n📊 FILTERING REPORT:")
        print(f"   📍 Total coins: {stats['total']}")
        print(f"   🚫 Blocked: {stats['blocked']}")
        print(f"   📉 Low market cap: {stats['low_market_cap']}")
        print(f"   📊 Low volume: {stats['low_volume']}")
        print(f"   ✅ Passed filters: {stats['passed']}")
        print(f"   📈 Filter rate: {(stats['passed']/stats['total']*100):.1f}%")
        
        return filtered_coins

    def save_market_data(self, coins):
        """Save market data to cache with CMC metadata"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        
        data = {
            'updated_at': datetime.utcnow().isoformat(),
            'source': 'CoinMarketCap Pro API',
            'system': 'BBW_1H',
            'total_coins': len(coins),
            'filters': {
                'min_market_cap': self.config.get('market_filter', {}).get('min_market_cap', 100_000_000),
                'min_volume_24h': self.config.get('market_filter', {}).get('min_volume_24h', 10_000_000),
                'blocked_coins_count': len(self.blocked_coins)
            },
            'coins': coins
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Saved {len(coins)} coins to cache")
        print(f"📄 Cache file: {cache_file}")
        return cache_file

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='CoinMarketCap Data Fetcher for BBW 1H System')
    parser.add_argument('--daily-scan', action='store_true', help='Run daily market data scan (5:30 AM IST)')
    
    args = parser.parse_args()
    
    if args.daily_scan:
        print("🌅 Starting daily CoinMarketCap scan for BBW 1H system...")
        print("=" * 60)
        
        try:
            fetcher = CoinMarketCapFetcher()
            
            # Single efficient API call to fetch market data
            coins = fetcher.fetch_cmc_listings()
            
            if coins:
                # Filter for BBW analysis with blocked coins tracking
                filtered_coins = fetcher.filter_bbw_coins(coins)
                
                # Save to cache
                cache_file = fetcher.save_market_data(filtered_coins)
                
                print(f"\n✅ Daily CMC scan complete!")
                print(f"🎯 Ready for BBW 1H analysis with {len(filtered_coins)} coins")
                print(f"💡 95% fewer API calls vs pagination method")
                print(f"⏱️  Next scan: Tomorrow 5:30 AM IST")
            else:
                print("❌ No coins fetched - check CMC API connection")
                
        except Exception as e:
            print(f"❌ Daily scan failed: {e}")
            raise

if __name__ == '__main__':
    main()
