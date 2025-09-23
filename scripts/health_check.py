#!/usr/bin/env python3
"""
Multi-Indicator System Health Check
Run this to verify all components are working
"""

import os
import sys
import json
import requests
from datetime import datetime

def check_environment_variables():
    """Check if all required environment variables are set"""
    required_vars = [
        'COINMARKETCAP_API_KEY',
        'BINGX_API_KEY', 
        'BINGX_SECRET_KEY',
        'TELEGRAM_BOT_TOKEN',
        'CIPHERB_TELEGRAM_CHAT_ID',
        'BBW_TELEGRAM_CHAT_ID',
        'SMA_TELEGRAM_CHAT_ID'
    ]
    
    print("🔍 Checking Environment Variables...")
    missing = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
            print(f"❌ {var}: Missing")
        else:
            print(f"✅ {var}: Set")
    
    if missing:
        print(f"\n⚠️ Missing {len(missing)} environment variables")
        return False
    
    print("✅ All environment variables set")
    return True

def check_cache_files():
    """Check if cache files exist"""
    print("\n🗂️ Checking Cache Files...")
    
    cache_files = [
        'cache/cipherb_dataset.json',
        'cache/sma_dataset.json'
    ]
    
    optional_files = [
        'cache/bbw_alerts.json', 
        'cache/sma_alerts.json'
    ]
    
    all_good = True
    
    for file in cache_files:
        if os.path.exists(file):
            with open(file) as f:
                data = json.load(f)
                coins = len(data.get('coins', []))
                print(f"✅ {file}: {coins} coins")
        else:
            print(f"❌ {file}: Missing (run daily data fetcher)")
            all_good = False
    
    for file in optional_files:
        if os.path.exists(file):
            print(f"✅ {file}: Exists")
        else:
            print(f"📝 {file}: Will be created on first run")
    
    return all_good

def check_coinmarketcap_api():
    """Test CoinMarketCap API"""
    print("\n💰 Testing CoinMarketCap API...")
    
    api_key = os.getenv('COINMARKETCAP_API_KEY')
    if not api_key:
        print("❌ CoinMarketCap API key not set")
        return False
    
    try:
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        params = {'limit': 5}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            print("✅ CoinMarketCap API: Working")
            return True
        else:
            print("❌ CoinMarketCap API: No data returned")
            return False
            
    except Exception as e:
        print(f"❌ CoinMarketCap API: {str(e)[:50]}")
        return False

def check_bingx_api():
    """Test BingX API"""
    print("\n🏦 Testing BingX API...")
    
    api_key = os.getenv('BINGX_API_KEY')
    if not api_key:
        print("❌ BingX API key not set")
        return False
    
    try:
        url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        headers = {'X-BX-APIKEY': api_key}
        params = {'symbol': 'BTC-USDT', 'interval': '1h', 'limit': 5}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('code') == 0 and data.get('data'):
            print("✅ BingX API: Working")
            return True
        else:
            print("❌ BingX API: Invalid response")
            return False
            
    except Exception as e:
        print(f"❌ BingX API: {str(e)[:50]}")
        return False

def check_telegram_bot():
    """Test Telegram Bot"""
    print("\n🤖 Testing Telegram Bot...")
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        print("❌ Telegram bot token not set")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{bot_token}/getMe"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('ok'):
            bot_name = data['result']['username']
            print(f"✅ Telegram Bot: @{bot_name} is active")
            return True
        else:
            print("❌ Telegram Bot: Invalid token")
            return False
            
    except Exception as e:
        print(f"❌ Telegram Bot: {str(e)[:50]}")
        return False

def main():
    print("🏥 MULTI-INDICATOR SYSTEM HEALTH CHECK")
    print("="*50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    checks = [
        check_environment_variables(),
        check_cache_files(),
        check_coinmarketcap_api(), 
        check_bingx_api(),
        check_telegram_bot()
    ]
    
    passed = sum(checks)
    total = len(checks)
    
    print(f"\n{'='*50}")
    print(f"🏥 HEALTH CHECK RESULTS: {passed}/{total} PASSED")
    
    if passed == total:
        print("✅ SYSTEM READY FOR PRODUCTION")
        print("🚀 You can now enable GitHub Actions workflows")
    else:
        print("⚠️ SYSTEM NEEDS ATTENTION")
        print("🔧 Fix the issues above before running")
    
    print("="*50)
    
    return passed == total

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
