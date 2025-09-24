#!/usr/bin/env python3
"""
Daily Market Data Fetcher - FIXED Blocked Coins Filtering
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
        
        # LOAD BLOCKED COINS
        self.blocked_coins = self._load_blocked_coins()
        
        print(f"📊 DataFetcher initialized - Cache dir: {self.cache_dir}")
        print(f"🚫 Blocked coins loaded: {len(self.blocked_coins)}")

    def _load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return {
                'cipherb_config': {'filters': {'min_market_cap': 100000000, 'min_volume_24h': 10000000}},
                'sma_config': {'filters': {'min_market_cap': 10000000, 'min_volume_24h': 10000000}}
            }

    def _load_blocked_coins(self):
        """FIXED: Load blocked coins from blocked_coins.txt"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins: {', '.join(list(blocked_coins)[:10])}...")
            return blocked_coins
            
        except FileNotFoundError:
            print("❌ blocked_coins.txt not found")
            return set()
        except Exception as e:
            print(f"❌ Error loading blocked coins: {e}")
            return set()

    def _is_blocked_coin(self, symbol: str) -> bool:
        """Check if coin is blocked"""
        return symbol.upper() in self.blocked_coins

    def fetch_all_coins(self) -> list:
        """Fetch unlimited coins from CoinMarketCap"""
        print("🚀 Starting unlimited CoinMarketCap fetch...")
        
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        all_coins = []
        start = 1
        limit = 5000
        
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
                    print(f"❌ No data in API response")
                    break
                
                coins = data['data']
                if not coins:
                    print(f"✅ No more coins to fetch")
                    break
                
                print(f"📊 Retrieved {len(coins)} coins")
                
                # Process and filter each coin
                filtered_count = 0
                blocked_count = 0
                
                for coin in coins:
                    try:
                        symbol = coin.get('symbol', '').upper()
                        
                        # APPLY BLOCKED COINS FILTER FIRST
                        if self._is_blocked_coin(symbol):
                            blocked_count += 1
                            print(f"🚫 Blocked: {symbol}")
                            continue
                        
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
                        
                        # Filter out coins with missing data
                        if (processed_coin['market_cap'] and 
                            processed_coin['current_price'] and 
                            processed_coin['total_volume']):
                            all_coins.append(processed_coin)
                        else:
                            filtered_count += 1
                            
                    except Exception as e:
                        print(f"⚠️ Error processing coin {coin.get('symbol', 'UNKNOWN')}: {e}")
                        continue
                
                print(f"🛡️ Blocked {blocked_count} coins, filtered {filtered_count} invalid coins")
                
                if len(coins) < limit:
                    print(f"✅ Reached end of available coins")
                    break
                    
                start += limit
                
                if len(all_coins) >= 10000:
                    print(f"⚠️ Safety limit reached: {len(all_coins)} coins")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ API request failed: {e}")
                break
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                break
        
        print(f"🎯 Total coins fetched (after blocking): {len(all_coins)}")
        return all_coins

    def create_filtered_datasets(self, all_coins: list) -> tuple:
        """Create filtered datasets"""
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
        
        print(f"✅ CipherB/BBW dataset: {len(cipherb_coins)} coins")
        print(f"✅ SMA dataset: {len(sma_coins)} coins")
        
        return cipherb_coins, sma_coins

    def save_datasets(self, cipherb_coins: list, sma_coins: list) -> tuple:
        """Save datasets to cache files"""
        print("💾 Saving datasets to cache...")
        
        try:
            # Save CipherB dataset
            cipherb_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_coins': len(cipherb_coins),
                'filters_applied': 'Market cap ≥ $100M, Volume ≥ $10M, Blocked coins filtered',
                'coins': cipherb_coins
            }
            
            cipherb_file = os.path.join(self.cache_dir, 'cipherb_dataset.json')
            with open(cipherb_file, 'w') as f:
                json.dump(cipherb_data, f, indent=2)
            
            # Save SMA dataset
            sma_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_coins': len(sma_coins),
                'filters_applied': 'Market cap ≥ $10M, Volume ≥ $10M, Blocked coins filtered',
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
    print("=" * 60)
    print("📊 STARTING UNLIMITED DAILY MARKET SCAN")
    print("=" * 60)
    
    try:
        fetcher = DataFetcher()
        all_coins = fetcher.fetch_all_coins()
        
        if all_coins:
            cipherb_filtered, sma_filtered = fetcher.create_filtered_datasets(all_coins)
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
