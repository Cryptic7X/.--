#!/usr/bin/env python3
"""
SIMPLE Data Fetcher - No complex logging, just basic functionality
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
        
        # Load blocked coins
        self.blocked_coins = self._load_blocked_coins()
        print(f"Blocked coins: {len(self.blocked_coins)}")

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
        """Fetch coins from CoinMarketCap - SIMPLE"""
        print("Fetching coins from CoinMarketCap...")
        
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        headers = {'X-CMC_PRO_API_KEY': self.api_key}
        
        all_coins = []
        for start in [1, 5001]:  # Get first 10,000 coins max
            try:
                params = {
                    'start': start,
                    'limit': 5000,
                    'sort': 'market_cap'
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=30)
                data = response.json()
                
                if 'data' not in data or not data['data']:
                    break
                    
                for coin in data['data']:
                    symbol = coin.get('symbol', '').upper()
                    
                    # Skip blocked coins - SIMPLE CHECK
                    if symbol in self.blocked_coins:
                        continue
                    
                    quote = coin.get('quote', {}).get('USD', {})
                    market_cap = quote.get('market_cap', 0)
                    volume = quote.get('volume_24h', 0)
                    price = quote.get('price', 0)
                    
                    # Only valid coins
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
                
                if len(data['data']) < 5000:
                    break
                    
            except Exception as e:
                print(f"Error fetching batch: {e}")
                break
        
        print(f"Total coins fetched: {len(all_coins)}")
        return all_coins

    def filter_coins(self, all_coins):
        """Filter coins into CipherB and SMA datasets - SIMPLE"""
        
        # CipherB/BBW: Market cap ≥ $100M, Volume ≥ $10M
        cipherb_coins = [
            coin for coin in all_coins
            if coin['market_cap'] >= 100_000_000 and coin['total_volume'] >= 10_000_000
        ]
        
        # SMA: Market cap ≥ $10M, Volume ≥ $10M  
        sma_coins = [
            coin for coin in all_coins
            if coin['market_cap'] >= 10_000_000 and coin['total_volume'] >= 10_000_000
        ]
        
        print(f"CipherB/BBW coins: {len(cipherb_coins)}")
        print(f"SMA coins: {len(sma_coins)}")
        
        return cipherb_coins, sma_coins

    def save_datasets(self, cipherb_coins, sma_coins):
        """Save datasets - SIMPLE"""
        os.makedirs('cache', exist_ok=True)
        
        # Save CipherB dataset
        cipherb_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_coins': len(cipherb_coins),
            'coins': cipherb_coins
        }
        
        with open('cache/cipherb_dataset.json', 'w') as f:
            json.dump(cipherb_data, f)
        
        # Save SMA dataset
        sma_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_coins': len(sma_coins),
            'coins': sma_coins
        }
        
        with open('cache/sma_dataset.json', 'w') as f:
            json.dump(sma_data, f)
        
        print("Datasets saved to cache/")

def main():
    print("Starting daily data collection...")
    
    fetcher = SimpleDataFetcher()
    all_coins = fetcher.fetch_coins()
    
    if all_coins:
        cipherb_coins, sma_coins = fetcher.filter_coins(all_coins)
        fetcher.save_datasets(cipherb_coins, sma_coins)
        print("✅ Data collection complete")
    else:
        print("❌ No coins fetched")

if __name__ == '__main__':
    main()
