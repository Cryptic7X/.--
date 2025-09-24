#!/usr/bin/env python3
"""
Daily Market Data Fetcher - PROPERLY FILTERS BLOCKED COINS
"""
import os
import sys
import json
import requests
import yaml
from datetime import datetime

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
        
        self.cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # LOAD BLOCKED COINS - CRITICAL FIX
        self.blocked_coins = self._load_blocked_coins()
        
        print(f"📊 DataFetcher initialized")
        print(f"🚫 Blocked {len(self.blocked_coins)} coins: {', '.join(list(self.blocked_coins)[:10])}...")

    def _load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except:
            return {
                'cipherb_config': {'filters': {'min_market_cap': 100000000, 'min_volume_24h': 10000000}},
                'sma_config': {'filters': {'min_market_cap': 10000000, 'min_volume_24h': 10000000}}
            }

    def _load_blocked_coins(self):
        """Load blocked coins from config/blocked_coins.txt"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    # Skip empty lines and comments
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins from {blocked_file}")
            return blocked_coins
            
        except FileNotFoundError:
            print(f"❌ {blocked_file} not found!")
            return set()
        except Exception as e:
            print(f"❌ Error loading blocked coins: {e}")
            return set()

    def _is_coin_blocked(self, symbol: str) -> bool:
        """Check if coin symbol is in blocked list"""
        return symbol.upper() in self.blocked_coins

    def fetch_all_coins(self) -> list:
        """Fetch all coins and FILTER OUT BLOCKED COINS"""
        print("🚀 Starting unlimited CoinMarketCap fetch...")
        
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        all_coins = []
        start = 1
        limit = 5000
        
        total_blocked = 0
        total_processed = 0
        
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
                
                if 'data' not in data or not data['data']:
                    print("✅ No more coins to fetch")
                    break
                
                coins = data['data']
                print(f"📊 Retrieved {len(coins)} coins from API")
                
                # Process each coin and APPLY BLOCKING FILTER
                for coin in coins:
                    try:
                        total_processed += 1
                        symbol = coin.get('symbol', '').upper()
                        
                        # CRITICAL: Check if coin is blocked FIRST
                        if self._is_coin_blocked(symbol):
                            total_blocked += 1
                            print(f"🚫 BLOCKED: {symbol}")
                            continue  # SKIP this coin completely
                        
                        # Process non-blocked coin
                        processed_coin = {
                            'id': coin.get('id'),
                            'symbol': symbol,
                            'name': coin.get('name'),
                            'market_cap': coin.get('quote', {}).get('USD', {}).get('market_cap', 0),
                            'current_price': coin.get('quote', {}).get('USD', {}).get('price', 0),
                            'total_volume': coin.get('quote', {}).get('USD', {}).get('volume_24h', 0),
                            'price_change_percentage_24h': coin.get('quote', {}).get('USD', {}).get('percent_change_24h', 0),
                            'last_updated': coin.get('last_updated')
                        }
                        
                        # Only add coins with valid data
                        if (processed_coin['market_cap'] > 0 and 
                            processed_coin['current_price'] > 0 and 
                            processed_coin['total_volume'] > 0):
                            all_coins.append(processed_coin)
                            print(f"✅ ADDED: {symbol}")
                        else:
                            print(f"⚠️ SKIPPED (invalid data): {symbol}")
                            
                    except Exception as e:
                        print(f"⚠️ Error processing {coin.get('symbol', 'UNKNOWN')}: {e}")
                        continue
                
                # Break conditions
                if len(coins) < limit:
                    print("✅ Reached end of available coins")
                    break
                    
                start += limit
                
                # Safety limit
                if len(all_coins) >= 8000:
                    print(f"⚠️ Safety limit reached: {len(all_coins)} coins")
                    break
                    
            except Exception as e:
                print(f"❌ API request failed: {e}")
                break
        
        print(f"🎯 FILTERING SUMMARY:")
        print(f"   📊 Total processed: {total_processed}")
        print(f"   🚫 Total blocked: {total_blocked}")
        print(f"   ✅ Final dataset: {len(all_coins)} coins")
        
        return all_coins

    def create_filtered_datasets(self, all_coins: list) -> tuple:
        """Create CipherB and SMA datasets"""
        print("🔧 Creating filtered datasets...")
        
        # CipherB/BBW filters
        cipherb_filters = self.config['cipherb_config']['filters']
        cipherb_coins = [
            coin for coin in all_coins 
            if (coin['market_cap'] >= cipherb_filters['min_market_cap'] and 
                coin['total_volume'] >= cipherb_filters['min_volume_24h'])
        ]
        
        # SMA filters
        sma_filters = self.config['sma_config']['filters']
        sma_coins = [
            coin for coin in all_coins
            if (coin['market_cap'] >= sma_filters['min_market_cap'] and 
                coin['total_volume'] >= sma_filters['min_volume_24h'])
        ]
        
        print(f"✅ CipherB/BBW dataset: {len(cipherb_coins)} coins (≥${cipherb_filters['min_market_cap']:,} cap, ≥${cipherb_filters['min_volume_24h']:,} vol)")
        print(f"✅ SMA dataset: {len(sma_coins)} coins (≥${sma_filters['min_market_cap']:,} cap, ≥${sma_filters['min_volume_24h']:,} vol)")
        
        return cipherb_coins, sma_coins

    def save_datasets(self, cipherb_coins: list, sma_coins: list):
        """Save datasets with timestamp"""
        print("💾 Saving datasets to cache...")
        
        try:
            timestamp = datetime.utcnow().isoformat()
            
            # CipherB dataset
            cipherb_data = {
                'timestamp': timestamp,
                'total_coins': len(cipherb_coins),
                'filters_applied': 'Market cap ≥ $100M, Volume ≥ $10M, Blocked coins removed',
                'coins': cipherb_coins
            }
            
            cipherb_file = os.path.join(self.cache_dir, 'cipherb_dataset.json')
            with open(cipherb_file, 'w') as f:
                json.dump(cipherb_data, f, indent=2)
            
            # SMA dataset
            sma_data = {
                'timestamp': timestamp,
                'total_coins': len(sma_coins),
                'filters_applied': 'Market cap ≥ $10M, Volume ≥ $10M, Blocked coins removed',
                'coins': sma_coins
            }
            
            sma_file = os.path.join(self.cache_dir, 'sma_dataset.json')
            with open(sma_file, 'w') as f:
                json.dump(sma_data, f, indent=2)
            
            print(f"✅ Datasets saved successfully")
            return cipherb_file, sma_file
            
        except Exception as e:
            print(f"❌ Error saving datasets: {e}")
            return None, None

def main():
    print("=" * 60)
    print("📊 STARTING DAILY MARKET SCAN WITH BLOCKED COIN FILTERING")
    print("=" * 60)
    
    try:
        fetcher = DataFetcher()
        all_coins = fetcher.fetch_all_coins()
        
        if all_coins:
            cipherb_filtered, sma_filtered = fetcher.create_filtered_datasets(all_coins)
            fetcher.save_datasets(cipherb_filtered, sma_filtered)
            
            print("\n" + "=" * 60)
            print("✅ DAILY SCAN COMPLETE - BLOCKED COINS FILTERED!")
            print(f"📊 CipherB/BBW Coins: {len(cipherb_filtered)}")
            print(f"📊 SMA Coins: {len(sma_filtered)}")
            print("=" * 60)
        else:
            print("❌ No coins fetched")
            
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
