"""
BBW 1H Fresh Analysis System
Detects BBW squeezes on 1-hour timeframe with fresh signal validation
Simplified exchange setup: BingX (Primary) → KuCoin → OKX
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
    try:
        import yaml
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        return config
    except Exception as e:
        print(f"⚠️ Config loading error: {e}")
        return get_default_config()

def get_default_config():
    """Default configuration"""
    return {
        'processing': {'batch_size': 50},
        'market_filter': {
            'min_market_cap': 50000000,
            'min_volume_24h': 10000000
        }
    }

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
    """Create simplified exchange connections (BingX → KuCoin → OKX only)"""
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
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
            print("✅ BingX connected (Primary)")
        except Exception as e:
            print(f"⚠️ BingX connection failed: {e}")
    
    # KuCoin (First Fallback)
    try:
        kucoin = ccxt.kucoin({
            'enableRateLimit': True,
            'timeout': 30000,
        })
        exchanges.append(('KuCoin', kucoin))
        print("✅ KuCoin connected (Fallback 1)")
    except Exception as e:
        print(f"⚠️ KuCoin connection failed: {e}")
    
    # OKX (Second Fallback)
    try:
        okx = ccxt.okx({
            'enableRateLimit': True,
            'timeout': 30000,
        })
        exchanges.append(('OKX', okx))
        print("✅ OKX connected (Fallback 2)")
    except Exception as e:
        print(f"⚠️ OKX connection failed: {e}")
    
    return exchanges

def clean_symbol(symbol):
    """Clean and validate symbol format"""
    if not symbol or not isinstance(symbol, str):
        return None
    
    # Remove invalid characters and standardize
    symbol = symbol.upper().strip()
    
    # Skip problematic symbols
    problematic_symbols = {
        'USDT0', 'USD0', 'NULL', 'NONE', '', 'UNKNOWN'
    }
    
    if symbol in problematic_symbols or len(symbol) < 2:
        return None
    
    return symbol

def get_symbol_variants(base_symbol):
    """Get trading symbol variants for different exchanges"""
    clean_base = clean_symbol(base_symbol)
    if not clean_base:
        return []
    
    # Generate common trading pairs
    variants = []
    
    # Standard variants
    if not clean_base.endswith('USDT') and not clean_base.endswith('USD'):
        variants.extend([
            f"{clean_base}USDT",
            f"{clean_base}USD",
            f"{clean_base}/USDT",
            f"{clean_base}/USD"
        ])
    
    return variants

def fetch_ohlcv_data(symbol, exchange_name, exchange, limit=150):
    """
    Fetch 1-hour OHLCV data for BBW calculation
    Need 125 + 20 + buffer = 150 candles minimum
    """
    try:
        # Fetch 1-hour candlesticks
        ohlcv = exchange.fetch_ohlcv(symbol, '1h', limit=limit)
        
        if not ohlcv or len(ohlcv) < 145:  # Need minimum data for BBW
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        return df
        
    except Exception as e:
        # Only print error for debugging, don't spam console
        if 'does not have market symbol' not in str(e):
            print(f"❌ Error fetching {symbol} from {exchange_name}: {e}")
        return None

def analyze_bbw_signals(coins, exchanges):
    """Analyze BBW signals for given coins"""
    signals = []
    blocked_coins = get_blocked_coins()
    
    print(f"🔍 Analyzing {len(coins)} coins for BBW squeezes...")
    
    # Process in batches of 50
    batch_size = 50
    processed_count = 0
    
    for i in range(0, len(coins), batch_size):
        batch = coins[i:i + batch_size]
        print(f"\n📊 Processing batch {i//batch_size + 1}/{(len(coins) + batch_size - 1)//batch_size}")
        
        for coin in batch:
            try:
                symbol_base = coin.get('symbol', '').upper()
                
                # Skip invalid or blocked coins
                if not symbol_base or symbol_base in blocked_coins:
                    continue
                
                # Get symbol variants
                symbol_variants = get_symbol_variants(symbol_base)
                
                if not symbol_variants:
                    continue
                
                ohlcv_data = None
                used_exchange = None
                used_symbol = None
                
                # Try each exchange in fallback sequence
                for exchange_name, exchange in exchanges:
                    if ohlcv_data is not None:
                        break
                        
                    for symbol_variant in symbol_variants:
                        try:
                            if exchange.has.get('fetchOHLCV', False):
                                ohlcv_data = fetch_ohlcv_data(symbol_variant, exchange_name, exchange)
                                if ohlcv_data is not None:
                                    used_exchange = exchange_name
                                    used_symbol = symbol_variant
                                    break
                        except Exception:
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
                    
                    # Calculate 24h change safely
                    try:
                        price_24h_ago = ohlcv_data['close'].iloc[-25] if len(ohlcv_data) >= 25 else ohlcv_data['close'].iloc[0]
                        change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
                    except:
                        change_24h = 0.0
                    
                    signal = {
                        'symbol': used_symbol,
                        'exchange': used_exchange,
                        'price': float(current_price),
                        'change_24h': float(change_24h),
                        'market_cap': coin.get('market_cap', 0),
                        'volume_24h': coin.get('total_volume', 0),
                        'bbw_value': float(squeeze_result['bbw_value']),
                        'lowest_contraction': float(squeeze_result['lowest_contraction']),
                        'difference': float(squeeze_result['difference']),
                        'strength': squeeze_result['strength'],
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    signals.append(signal)
                    print(f"🎯 BBW Squeeze: {used_symbol} - BBW: {squeeze_result['bbw_value']:.4f} - {squeeze_result['strength']}")
                
                processed_count += 1
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error analyzing {coin.get('symbol', 'Unknown')}: {e}")
                continue
    
    print(f"\n📈 Processed {processed_count} coins, found {len(signals)} BBW squeezes")
    return signals

def filter_fresh_signals(signals):
    """Filter signals using deduplication logic"""
    cache = load_alert_cache()
    fresh_signals = []
    
    for signal in signals:
        # Use 1-hour cache key
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
            print("💡 Run: python src/data_fetcher.py --daily-scan")
            return
        
        # Filter by market cap and volume (from your document)
        min_market_cap = config.get('market_filter', {}).get('min_market_cap', 50_000_000)
        min_volume_24h = config.get('market_filter', {}).get('min_volume_24h', 10_000_000)
        
        filtered_coins = [
            coin for coin in coins 
            if (coin.get('market_cap', 0) or 0) >= min_market_cap and  
               (coin.get('total_volume', 0) or 0) >= min_volume_24h
        ]
        
        print(f"📊 Loaded {len(filtered_coins)} coins (Market Cap ≥ ${min_market_cap:,}, Volume ≥ ${min_volume_24h:,})")
        
        # Create exchange connections (simplified)
        exchanges = create_exchange_connections()
        if not exchanges:
            print("❌ No exchange connections available")
            return
        
        print(f"🔗 Connected exchanges: {[name for name, _ in exchanges]}")
        
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
            
            print("✅ Fresh signals sent and cache updated")
            
        else:
            print("ℹ️ All signals were duplicates (already alerted)")
        
        # Cleanup old cache entries
        cleanup_old_alerts()
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
