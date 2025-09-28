#!/usr/bin/env python3
"""
SimpleDataFetcher - OPTIMIZED for Top 1500 Coins
✅ Uses limit=500 for maximum credit efficiency  
✅ Fetches only top 1500 coins (covers all your targets)
✅ Configuration-driven filtering from config.yaml
✅ Reduces daily credits from 23 to 12 (48% savings!)
"""

import os
import json
import requests
import yaml
from datetime import datetime

class SimpleDataFetcher:
    def __init__(self):
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        if not self.api_key:
            print("❌ COINMARKETCAP_API_KEY not set")
            exit(1)
        
        # Load configuration from config.yaml
        self.config = self._load_config()
        self.blocked_coins = self._load_blocked_coins()
        print(f"🛑 Blocked coins loaded: {len(self.blocked_coins)}")
        
        # Display filter settings
        print(f"📊 CipherB/BBW Filter: Market Cap ≥ ${self.config['market_filters']['cipherb_bbw']['min_market_cap']:,}, Volume ≥ ${self.config['market_filters']['cipherb_bbw']['min_volume_24h']:,}")
        print(f"📊 EMA Filter: Market Cap ${self.config['market_filters']['ema']['min_market_cap']:,} - ${self.config['market_filters']['ema']['max_market_cap']:,}, Volume ≥ ${self.config['market_filters']['ema']['min_volume_24h']:,}")
    
    def _load_config(self):
        """Load configuration from config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            # Fallback to hard-coded values if config fails
            return {
                'market_filters': {
                    'cipherb_bbw': {
                        'min_market_cap': 200000000,
                        'min_volume_24h': 20000000
                    },
                    'ema': {
                        'min_market_cap': 10000000,
                        'max_market_cap': 200000000,
                        'min_volume_24h': 10000000
                    }
                }
            }
    
    def _load_blocked_coins(self):
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked = set()
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked.add(coin)
        except:
            pass
        return blocked

    def fetch_coins(self):
        print("🚀 Fetching top 1500 coins from CoinMarketCap...")
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        headers = {'X-CMC_PRO_API_KEY': self.api_key}
        all_coins = []
        
        # OPTIMIZED: 3 calls × 500 limit = top 1500 coins
        # Credit calculation: 3 × (1 base + 500÷200) = 3 × (1 + 3) = 12 credits
        for start in [1, 501, 1001]:  # Top 1500 coins only
            try:
                params = {
                    'start': start,
                    'limit': 500,           # Optimal credit efficiency
                    'sort': 'market_cap',   # Ensures we get top coins first
                    'convert': 'USD'
                    # NO server-side filtering - unreliable
                }
                
                print(f"📡 Fetching coins {start}-{start+499}...")
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if 'data' not in data or not data['data']:
                    print(f"⚠️ No data received for start={start}")
                    break
                    
                batch_count = 0
                for coin in data['data']:
                    symbol = coin.get('symbol', '').upper()
                    if symbol in self.blocked_coins:
                        continue
                    
                    quote = coin.get('quote', {}).get('USD', {})
                    market_cap = quote.get('market_cap', 0)
                    volume = quote.get('volume_24h', 0)
                    price = quote.get('price', 0)
                    
                    # Basic validation - ensure we have meaningful data
                    if market_cap and volume and price:
                        all_coins.append({
                            'id': coin.get('id'),
                            'symbol': symbol,
                            'name': coin.get('name'),
                            'market_cap': market_cap,
                            'current_price': price,
                            'total_volume': volume,
                            'price_change_percentage_24h': quote.get('percent_change_24h', 0),
                            'last_updated': coin.get('last_updated')
                        })
                        batch_count += 1
                
                print(f"✅ Processed {batch_count} valid coins from batch {start}")
                
                # Break if we got less than 500 (last page)
                if len(data['data']) < 500:
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Network error fetching batch {start}: {e}")
                break
            except Exception as e:
                print(f"❌ Error processing batch {start}: {e}")
                break
        
        print(f"📊 Total valid coins fetched: {len(all_coins)}")
        print(f"💰 Estimated credits used: ~12 (vs 23 previous)")
        return all_coins
    
    def filter_coins(self, all_coins):
        """Configuration-driven filtering using client-side logic"""
        
        # Get filter values from config
        cipherb_bbw_filters = self.config['market_filters']['cipherb_bbw']
        ema_filters = self.config['market_filters']['ema']
        
        # CipherB/BBW coins - using config values
        cipherb_coins = [
            coin for coin in all_coins
            if coin['market_cap'] >= cipherb_bbw_filters['min_market_cap'] 
            and coin['total_volume'] >= cipherb_bbw_filters['min_volume_24h']
        ]
        
        # EMA coins - using config values  
        ema_coins = [
            coin for coin in all_coins
            if ema_filters['min_market_cap'] <= coin['market_cap'] <= ema_filters['max_market_cap']
            and coin['total_volume'] >= ema_filters['min_volume_24h']
        ]
        
        print(f"📊 CipherB/BBW coins: {len(cipherb_coins)}")
        print(f"📊 EMA coins: {len(ema_coins)}")
        
        return cipherb_coins, ema_coins
    
    def save_datasets(self, cipherb_coins, ema_coins):
        """Save datasets to cache with timestamps"""
        os.makedirs('cache', exist_ok=True)
        timestamp = datetime.utcnow().isoformat()
        
        # CipherB dataset
        cipherb_data = {
            'timestamp': timestamp,
            'total_coins': len(cipherb_coins),
            'coins': cipherb_coins
        }
        
        with open('cache/cipherb_dataset.json', 'w') as f:
            json.dump(cipherb_data, f, indent=2)
        
        # EMA dataset  
        ema_data = {
            'timestamp': timestamp,
            'total_coins': len(ema_coins),
            'coins': ema_coins
        }
        
        with open('cache/ema_dataset.json', 'w') as f:
            json.dump(ema_data, f, indent=2)
        
        print("💾 Datasets saved to cache/")

def main():
    print("🚀 Starting optimized daily data collection...")
    print("📈 Targeting top 1500 coins with limit=500 for maximum efficiency")
    
    fetcher = SimpleDataFetcher()
    
    all_coins = fetcher.fetch_coins()
    if all_coins:
        cipherb_coins, ema_coins = fetcher.filter_coins(all_coins)
        fetcher.save_datasets(cipherb_coins, ema_coins)
        print("✅ Data collection complete!")
        print(f"💰 Credits saved: ~48% reduction (23 → 12 credits)")
    else:
        print("❌ No coins fetched")

if __name__ == "__main__":
    main()
