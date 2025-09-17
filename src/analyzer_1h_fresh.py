"""
BBW 1H Fresh Analysis System
Detects BBW squeezes on 1-hour timeframe with fresh signal validation
"""

import os
import sys
import json
import time
import ccxt
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from indicators.bbw_exact import calculate_bbw, detect_bbw_squeeze
from alerts.deduplication_fresh import (
    load_alert_cache, save_alert_cache, is_duplicate_alert, 
    cleanup_old_alerts, get_cache_key
)
from alerts.telegram_batch import send_batch_telegram_alert

def load_config():
    """Load configuration from config.yaml"""
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config

def load_market_data():
    """Load market data from CoinGecko cache"""
    cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
    market_data_file = os.path.join(cache_dir, 'high_risk_market_data.json')
    
    if not os.path.exists(market_data_file):
        print("❌ Market data file not found. Run data_fetcher.py first.")
        return []
    
    try:
        with open(market_data_file, 'r') as file:
            data = json.load(file)
            return data.get('coins', [])
    except Exception as e:
        print(f"❌ Error loading market data: {e}")
        return []

def get_blocked_coins():
    """Load blocked coins list"""
    blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
    
    if not os.path.exists(blocked_file):
        return set()
    
    try:
        with open(blocked_file, 'r') as file:
            blocked_coins = {line.strip().upper() for line in file if line.strip()}
        return blocked_coins
    except Exception as e:
        print(f"❌ Error loading blocked coins: {e}")
        return set()

def create_exchange_connections():
    """Create exchange connections with fallback sequence"""
    exchanges = []
    
    # BingX (Primary - Your Personal API)
    bingx_api_key = os.getenv('BINGX_API_KEY')
    bingx_secret = os.getenv('BINGX_SECRET_KEY')
    
    if bingx_api_key and bingx_secret:
        try:
            bingx = ccxt.bingx({
                'apiKey': bingx_api_key,
                'secret': bingx_secret,
                'sandbox': False,
                'enableRateLimit': True,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX connection failed: {e}")
    
    # Fallback exchanges (Public APIs)
    fallback_exchanges = [
        ('KuCoin', ccxt.kucoin, {'enableRateLimit': True}),
        ('OKX', ccxt.okx, {'enableRateLimit': True}),
        ('Bybit', ccxt.bybit, {'enableRateLimit': True}),
        ('Blofin', ccxt.blofin, {'enableRateLimit': True})
    ]
    
    for name, exchange_class, config in fallback_exchanges:
        try:
            exchange = exchange_class(config)
            exchanges.append((name, exchange))
        except Exception as e:
            print(f"⚠️ {name} connection failed: {e}")
    
    return exchanges

def fetch_ohlcv_data(symbol, exchange_name, exchange, limit=150):
    """
    Fetch 1-hour OHLCV data for BBW calculation
    Need 125 + 20 + buffer = 150 candles minimum
    """
    try:
        # Fetch 1-hour candlesticks (changed from 30m)
        ohlcv = exchange.fetch_ohlcv(symbol, '1h', limit=limit)
        
        if not ohlcv or len(ohlcv) < 145:  # Need minimum data for BBW
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching {symbol} from {exchange_name}: {e}")
        return None

def analyze_bbw_signals(coins, exchanges):
    """Analyze BBW signals for given coins"""
    signals = []
    blocked_coins = get_blocked_coins()
    
    print(f"🔍 Analyzing {len(coins)} coins for BBW squeezes...")
    
    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(coins), batch_size):
        batch = coins[i:i + batch_size]
        print(f"\n📊 Processing batch {i//batch_size + 1}/{(len(coins) + batch_size - 1)//batch_size}")
        
        for coin in batch:
            try:
                symbol_base = coin['symbol'].upper()
                
                # Skip blocked coins
                if symbol_base in blocked_coins:
                    continue
                
                # Try different symbol formats
                symbol_variants = [f"{symbol_base}USDT", f"{symbol_base}USD"]
                
                ohlcv_data = None
                used_exchange = None
                used_symbol = None
                
                # Try each exchange in fallback sequence
                for exchange_name, exchange in exchanges:
                    for symbol_variant in symbol_variants:
                        try:
                            if exchange.has['fetchOHLCV']:
                                ohlcv_data = fetch_ohlcv_data(symbol_variant, exchange_name, exchange)
                                if ohlcv_data is not None:
                                    used_exchange = exchange_name
                                    used_symbol = symbol_variant
                                    break
                        except Exception as e:
                            continue
                    
                    if ohlcv_data is not None:
                        break
                
                if ohlcv_data is None:
                    continue
                
                # Calculate BBW
                bbw_data = calculate_bbw(ohlcv_data)
                if not bbw_data:
                    continue
                
                # Detect squeeze
                squeeze_result = detect_bbw_squeeze(bbw_data, tolerance=0.01)
                
                if squeeze_result['is_squeeze']:
                    # Get current price and 24h change
                    current_price = ohlcv_data['close'].iloc[-1]
                    price_24h_ago = ohlcv_data['close'].iloc[-25] if len(ohlcv_data) >= 25 else ohlcv_data['close'].iloc[0]
                    change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
                    
                    signal = {
                        'symbol': used_symbol,
                        'exchange': used_exchange,
                        'price': current_price,
                        'change_24h': change_24h,
                        'market_cap': coin.get('market_cap', 0),
                        'volume_24h': coin.get('total_volume', 0),
                        'bbw_value': squeeze_result['bbw_value'],
                        'lowest_contraction': squeeze_result['lowest_contraction'],
                        'difference': squeeze_result['difference'],
                        'strength': squeeze_result['strength'],
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    signals.append(signal)
                    print(f"🎯 BBW Squeeze: {used_symbol} - BBW: {squeeze_result['bbw_value']:.4f}")
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                print(f"❌ Error analyzing {coin.get('symbol', 'Unknown')}: {e}")
                continue
    
    return signals

def filter_fresh_signals(signals):
    """Filter signals using deduplication logic"""
    cache = load_alert_cache()
    fresh_signals = []
    
    for signal in signals:
        # Use 1-hour cache key (changed from 30m)
        cache_key = get_cache_key(signal['symbol'], '1h')
        
        if not is_duplicate_alert(cache, cache_key, signal['timestamp']):
            fresh_signals.append(signal)
    
    return fresh_signals

def main():
    """Main analysis function"""
    print("🚀 Starting BBW 1H Fresh Analysis")
    print("=" * 50)
    
    try:
        # Load configuration
        config = load_config()
        
        # Load market data
        coins = load_market_data()
        if not coins:
            print("❌ No market data available")
            return
        
        # Filter by market cap and volume (from your document)
        filtered_coins = [
            coin for coin in coins 
            if coin.get('market_cap', 0) >= 50_000_000 and  # $50M min
               coin.get('total_volume', 0) >= 10_000_000     # $10M min
        ]
        
        print(f"📊 Loaded {len(filtered_coins)} coins (filtered by market cap/volume)")
        
        # Create exchange connections
        exchanges = create_exchange_connections()
        if not exchanges:
            print("❌ No exchange connections available")
            return
        
        print(f"🔗 Connected to {len(exchanges)} exchanges: {[name for name, _ in exchanges]}")
        
        # Analyze BBW signals
        signals = analyze_bbw_signals(filtered_coins, exchanges)
        
        if not signals:
            print("ℹ️ No BBW squeeze signals detected")
            return
        
        # Filter fresh signals
        fresh_signals = filter_fresh_signals(signals)
        
        if fresh_signals:
            print(f"\n🎯 Found {len(fresh_signals)} fresh BBW squeeze signals")
            
            # Send alerts
            send_batch_telegram_alert(fresh_signals, "BBW", "1H")
            
            # Update cache
            cache = load_alert_cache()
            for signal in fresh_signals:
                cache_key = get_cache_key(signal['symbol'], '1h')
                cache[cache_key] = {
                    'timestamp': signal['timestamp'],
                    'bbw_value': signal['bbw_value']
                }
            save_alert_cache(cache)
            
        else:
            print("ℹ️ All signals were duplicates (already alerted)")
        
        # Cleanup old cache entries
        cleanup_old_alerts()
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
