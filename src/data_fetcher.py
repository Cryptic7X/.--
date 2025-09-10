#!/usr/bin/env python3
"""
CoinGecko Market Data Fetcher for BBW 15-Minute Alert System
Fetches top 1,250 coins with API authentication and BBW-specific filtering
"""
import os
import json
import time
import requests
import yaml
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoinGeckoFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.session = self.create_robust_session()
        self.blocked_coins = self.load_blocked_coins()
        
    def load_config(self):
        """Load system configuration"""
        try:
            config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
            with open(config_path) as f:
                config = yaml.safe_load(f)
            logger.info("✅ Configuration loaded successfully")
            return config
        except Exception as e:
            logger.error(f"❌ Error loading config: {e}")
            # Fallback configuration for BBW system
            return {
                'market_data': {
                    'min_market_cap': 50_000_000,
                    'min_volume_24h': 10_000_000,
                    'max_coins': 1250
                },
                'cache': {
                    'market_data_file': 'high_risk_market_data.json'
                }
            }
    
    def load_blocked_coins(self):
        """Load blocked coins list"""
        try:
            blocked_file = Path(__file__).parent.parent / 'config' / 'blocked_coins.txt'
            with open(blocked_file, 'r') as f:
                blocked = {line.strip().upper() for line in f 
                          if line.strip() and not line.startswith('#')}
            logger.info(f"📋 Loaded {len(blocked)} blocked coins")
            return blocked
        except Exception as e:
            logger.warning(f"⚠️ Could not load blocked coins: {e}")
            # Default blocked coins for BBW system
            return {'USDT', 'USDC', 'BUSD', 'DAI', 'WBTC', 'WETH'}
    
    def create_robust_session(self):
        """Create requests session with API key authentication"""
        session = requests.Session()
        
        # Configure retry strategy for CoinGecko API
        retry_strategy = Retry(
            total=5,
            backoff_factor=3,  # Increased for better rate limit handling
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers with API key authentication
        session.headers.update({
            'User-Agent': 'BBW-15m-Alert-System/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Add CoinGecko Demo API key
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            session.headers['x-cg-demo-api-key'] = api_key
            logger.info("✅ Using CoinGecko Demo API Key")
            logger.info(f"   API Key: {api_key[:8]}...{api_key[-4:]}")
        else:
            logger.warning("⚠️ No CoinGecko API Key found - using public limits")
            logger.warning("   Set COINGECKO_API_KEY environment variable for better performance")
        
        return session
    
    def fetch_market_coins(self):
        """Fetch coins using Demo API key for BBW analysis"""
        base_url = "https://api.coingecko.com/api/v3"
        coins = []
        
        logger.info(f"🚀 Starting CoinGecko API fetch for BBW 15m system...")
        
        # Calculate pages needed for BBW system (target: 1,250 coins)
        max_coins = self.config.get('market_data', {}).get('max_coins', 1250)
        per_page = 250  # CoinGecko max per page
        pages = (max_coins + per_page - 1) // per_page  # Ceiling division
        
        logger.info(f"📊 Target: {pages} pages × {per_page} coins = {pages * per_page} total coins")
        
        for page in range(1, pages + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'price_change_percentage': '24h'
            }
            
            logger.info(f"📄 Fetching page {page}/{pages} (requesting {per_page} coins)")
            
            max_attempts = 4
            for attempt in range(max_attempts):
                try:
                    url = f"{base_url}/coins/markets"
                    response = self.session.get(url, params=params, timeout=90)
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', '60')
                        wait_time = int(retry_after)
                        logger.warning(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time + 2)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        logger.warning(f"📭 No data for page {page} - stopping")
                        break
                        
                    coins.extend(data)
                    logger.info(f"✅ Page {page}: {len(data)} coins fetched (total: {len(coins)})")
                    break
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ Request attempt {attempt + 1} failed: {str(e)[:100]}")
                    if attempt == max_attempts - 1:
                        logger.error(f"❌ All attempts failed for page {page}")
                        break
                    time.sleep((2 ** attempt) + 1)
                except Exception as e:
                    logger.error(f"❌ Unexpected error on attempt {attempt + 1}: {str(e)[:100]}")
                    if attempt == max_attempts - 1:
                        break
                    time.sleep((2 ** attempt) + 1)
            
            # Rate limiting between pages (increased for BBW system stability)
            if page < pages:
                rate_limit_delay = 3  # 3 seconds between pages
                logger.info(f"⏳ Waiting {rate_limit_delay}s before next page...")
                time.sleep(rate_limit_delay)
        
        logger.info(f"📊 Total coins fetched: {len(coins)}")
        return coins
    
    def filter_bbw_coins(self, coins):
        """Filter coins specifically for BBW 15-minute analysis"""
        logger.info(f"🔍 Filtering {len(coins)} coins for BBW 15m analysis...")
        
        filtered_coins = []
        blocked_count = 0
        insufficient_cap = 0
        insufficient_volume = 0
        
        min_market_cap = self.config.get('market_data', {}).get('min_market_cap', 50_000_000)
        min_volume_24h = self.config.get('market_data', {}).get('min_volume_24h', 10_000_000)
        
        for coin in coins:
            try:
                symbol = coin.get('symbol', '').upper()
                
                # Check if coin is blocked
                if symbol in self.blocked_coins:
                    blocked_count += 1
                    continue
                
                market_cap = coin.get('market_cap') or 0
                volume_24h = coin.get('total_volume') or 0
                
                # Market cap filter
                if market_cap < min_market_cap:
                    insufficient_cap += 1
                    continue
                
                # Volume filter  
                if volume_24h < min_volume_24h:
                    insufficient_volume += 1
                    continue
                
                # Additional quality checks for BBW analysis
                current_price = coin.get('current_price') or 0
                if current_price <= 0:
                    continue
                
                # Add BBW-specific metadata
                coin['bbw_eligible'] = True
                coin['filter_timestamp'] = datetime.utcnow().isoformat()
                
                filtered_coins.append(coin)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error filtering coin {coin.get('symbol', 'UNKNOWN')}: {e}")
                continue
        
        # Log filtering statistics
        logger.info(f"📊 BBW Filtering Results:")
        logger.info(f"   ✅ Qualified coins: {len(filtered_coins)}")
        logger.info(f"   🚫 Blocked coins: {blocked_count}")
        logger.info(f"   💰 Low market cap: {insufficient_cap}")
        logger.info(f"   📉 Low volume: {insufficient_volume}")
        logger.info(f"   📈 Market Cap ≥ ${min_market_cap:,}")
        logger.info(f"   📊 Volume ≥ ${min_volume_24h:,}")
        
        return filtered_coins
    
    def save_market_data(self, coins):
        """Save market data to cache for BBW system"""
        try:
            cache_dir = Path(__file__).parent.parent / 'cache'
            cache_dir.mkdir(exist_ok=True)
            
            cache_file = cache_dir / self.config.get('cache', {}).get('market_data_file', 'high_risk_market_data.json')
            
            # Create comprehensive cache data for BBW system
            cache_data = {
                'system': 'BBW-15m-Alert-System',
                'version': '1.0.0',
                'updated_at': datetime.utcnow().isoformat(),
                'next_update': (datetime.utcnow() + timedelta(hours=24)).isoformat(),
                'total_coins': len(coins),
                'filters_applied': {
                    'min_market_cap': self.config.get('market_data', {}).get('min_market_cap', 50_000_000),
                    'min_volume_24h': self.config.get('market_data', {}).get('min_volume_24h', 10_000_000),
                    'blocked_coins_count': len(self.blocked_coins)
                },
                'coins': coins
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            logger.info(f"💾 Saved {len(coins)} BBW-eligible coins to cache")
            logger.info(f"📄 Cache file: {cache_file}")
            
            # Create backup cache for reliability
            backup_file = cache_dir / f"market_data_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            with open(backup_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            return str(cache_file)
            
        except Exception as e:
            logger.error(f"❌ Error saving market data: {e}")
            raise
    
    def validate_cache_data(self, cache_file):
        """Validate saved cache data"""
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            coins = data.get('coins', [])
            if not coins:
                logger.error("❌ Cache validation failed: No coins in cache")
                return False
            
            # Validate sample coins
            sample_size = min(10, len(coins))
            valid_coins = 0
            
            for coin in coins[:sample_size]:
                if all(key in coin for key in ['symbol', 'current_price', 'market_cap', 'total_volume']):
                    valid_coins += 1
            
            validation_rate = (valid_coins / sample_size) * 100
            
            if validation_rate >= 90:
                logger.info(f"✅ Cache validation passed: {validation_rate:.1f}% valid coins")
                return True
            else:
                logger.error(f"❌ Cache validation failed: {validation_rate:.1f}% valid coins")
                return False
                
        except Exception as e:
            logger.error(f"❌ Cache validation error: {e}")
            return False

def main():
    """Main execution for BBW 15m system data fetcher"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CoinGecko Market Data Fetcher for BBW 15m System')
    parser.add_argument('--daily-scan', action='store_true', help='Run daily market scan for BBW analysis')
    parser.add_argument('--validate-cache', action='store_true', help='Validate existing cache data')
    
    args = parser.parse_args()
    
    try:
        if args.daily_scan:
            logger.info("🌅 Starting daily market data scan for BBW 15m system...")
            
            fetcher = CoinGeckoFetcher()
            
            # Fetch market data
            start_time = time.time()
            coins = fetcher.fetch_market_coins()
            
            if coins:
                # Filter for BBW analysis
                filtered_coins = fetcher.filter_bbw_coins(coins)
                
                if filtered_coins:
                    # Save to cache
                    cache_file = fetcher.save_market_data(filtered_coins)
                    
                    # Validate cache
                    if fetcher.validate_cache_data(cache_file):
                        execution_time = time.time() - start_time
                        
                        logger.info(f"\n✅ Daily BBW scan complete!")
                        logger.info(f"⏱️ Execution time: {execution_time:.1f} seconds")
                        logger.info(f"📄 Cache file: {cache_file}")
                        logger.info(f"🎯 {len(filtered_coins)} coins ready for BBW 15m analysis")
                        logger.info(f"🔄 Next update: 24 hours")
                    else:
                        logger.error("❌ Cache validation failed - data may be corrupted")
                        exit(1)
                else:
                    logger.error("❌ No coins passed BBW filtering criteria")
                    exit(1)
            else:
                logger.error("❌ No coins fetched - check API connection and key")
                exit(1)
        
        elif args.validate_cache:
            logger.info("🔍 Validating existing cache data...")
            fetcher = CoinGeckoFetcher()
            
            cache_file = Path(__file__).parent.parent / 'cache' / 'high_risk_market_data.json'
            
            if cache_file.exists():
                if fetcher.validate_cache_data(cache_file):
                    logger.info("✅ Cache validation successful")
                else:
                    logger.error("❌ Cache validation failed")
                    exit(1)
            else:
                logger.error(f"❌ Cache file not found: {cache_file}")
                exit(1)
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.info("🛑 Operation cancelled by user")
        exit(0)
    except Exception as e:
        logger.error(f"💥 Critical error: {e}", exc_info=True)
        exit(1)

if __name__ == '__main__':
    main()
