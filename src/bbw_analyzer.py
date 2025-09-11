#!/usr/bin/env python3
"""
BBW 15-Minute Alert System - Complete Single File
Exact Pine Script BBW calculation with all functionality included
"""

import asyncio
import json
import os
import time
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import ccxt.async_support as ccxt
import aiohttp
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BBWCalculator:
    """Exact Pine Script BBW calculation"""
    
    def __init__(self, length: int = 20, mult: float = 2.0, contraction_length: int = 125):
        self.length = length
        self.mult = mult
        self.contraction_length = contraction_length
    
    def calculate_bbw_exact(self, ohlcv_data: pd.DataFrame) -> Dict:
        """Calculate BBW exactly as Pine Script"""
        try:
            if len(ohlcv_data) < self.contraction_length:
                raise ValueError(f"Insufficient data: {len(ohlcv_data)} < {self.contraction_length}")
            
            close_prices = ohlcv_data['close']
            
            # Bollinger Bands (exact Pine Script logic)
            basis = close_prices.rolling(window=self.length, min_periods=self.length).mean()
            std_dev = close_prices.rolling(window=self.length, min_periods=self.length).std(ddof=1)
            dev = self.mult * std_dev
            upper = basis + dev
            lower = basis - dev
            
            # BBW calculation
            bbw = ((upper - lower) / basis) * 100
            
            # Rolling highest/lowest
            highest_expansion = bbw.rolling(window=self.contraction_length, min_periods=self.contraction_length).max()
            lowest_contraction = bbw.rolling(window=self.contraction_length, min_periods=self.contraction_length).min()
            
            # Remove NaN values
            valid_data = ~(bbw.isna() | highest_expansion.isna() | lowest_contraction.isna())
            
            if not valid_data.any():
                raise ValueError("No valid BBW data after calculation")
            
            return {
                'bbw': bbw[valid_data],
                'highest_expansion': highest_expansion[valid_data],
                'lowest_contraction': lowest_contraction[valid_data],
                'timestamps': ohlcv_data.index[valid_data]
            }
            
        except Exception as e:
            logger.error(f"❌ BBW calculation error: {e}")
            raise
    
    def detect_squeeze(self, bbw_data: Dict, tolerance: float = 0.001) -> Dict:
        """Detect BBW squeeze"""
        try:
            if len(bbw_data['bbw']) == 0:
                return {'is_squeeze': False, 'error': 'No BBW data'}
            
            current_bbw = bbw_data['bbw'].iloc[-1]
            current_lowest = bbw_data['lowest_contraction'].iloc[-1]
            current_highest = bbw_data['highest_expansion'].iloc[-1]
            
            difference = abs(current_bbw - current_lowest)
            is_squeeze = difference <= tolerance
            
            return {
                'is_squeeze': is_squeeze,
                'bbw_value': round(current_bbw, 4),
                'lowest_contraction': round(current_lowest, 4),
                'highest_expansion': round(current_highest, 4),
                'difference': round(difference, 6),
                'timestamp': bbw_data['timestamps'].iloc[-1]
            }
            
        except Exception as e:
            logger.error(f"❌ Squeeze detection error: {e}")
            return {'is_squeeze': False, 'error': str(e)}

class BBWAnalyzer:
    """Main BBW Analysis Engine"""
    
    def __init__(self):
        self.bbw_calc = BBWCalculator()
        self.alert_cache = {}
        self.blocked_coins = self.load_blocked_coins()
        
        # Initialize exchanges
        self.exchanges = self._setup_exchanges()
        
        # Telegram config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
        
    def load_blocked_coins(self) -> set:
        """Load blocked coins"""
        try:
            blocked_file = Path(__file__).parent.parent / 'config' / 'blocked_coins.txt'
            if blocked_file.exists():
                with open(blocked_file, 'r') as f:
                    blocked = {line.strip().upper() for line in f 
                              if line.strip() and not line.startswith('#')}
                logger.info(f"📋 Loaded {len(blocked)} blocked coins")
                return blocked
            else:
                return {'USDT', 'USDC', 'BUSD', 'DAI', 'WBTC', 'WETH'}
        except Exception as e:
            logger.warning(f"⚠️ Could not load blocked coins: {e}")
            return {'USDT', 'USDC', 'BUSD', 'DAI', 'WBTC', 'WETH'}
    
    def _setup_exchanges(self) -> List:
        """Setup exchanges"""
        exchanges = []
        
        # BingX
        if os.getenv('BINGX_API_KEY'):
            exchanges.append(ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY'),
                'secret': os.getenv('BINGX_SECRET_KEY'),
                'sandbox': False,
                'rateLimit': 1200,
                'enableRateLimit': True,
            }))
        
        # KuCoin fallback
        exchanges.append(ccxt.kucoin({
            'rateLimit': 1200,
            'enableRateLimit': True,
        }))
        
        return exchanges
    
    async def load_market_data(self) -> Optional[List[Dict]]:
        """Load cached market data"""
        try:
            cache_file = Path(__file__).parent.parent / 'cache' / 'high_risk_market_data.json'
            if not cache_file.exists():
                logger.error("❌ Market data cache not found. Skipping analysis.")
                return None
                
            with open(cache_file, 'r') as f:
                data = json.load(f)
                
            # Handle both old and new cache formats
            if isinstance(data, dict) and 'coins' in data:
                coins = data['coins']
            else:
                coins = data
                
            logger.info(f"✅ Loaded {len(coins)} coins from cache")
            return coins
            
        except Exception as e:
            logger.error(f"❌ Error loading market data: {e}")
            return None
    
    async def fetch_ohlcv_data(self, symbol: str, timeframe: str = '15m', limit: int = 150) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data with fallback"""
        for exchange in self.exchanges:
            try:
                await exchange.load_markets()
                if symbol not in exchange.markets:
                    continue
                
                ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                
                if len(ohlcv) < 125:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                return df
                
            except Exception as e:
                logger.debug(f"❌ {symbol} failed on {exchange.name}: {e}")
                continue
        
        return None
    
    async def analyze_single_coin(self, coin: Dict, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Analyze single coin"""
        async with semaphore:
            try:
                symbol = f"{coin['symbol'].upper()}/USDT"
                
                # Fetch 15m data
                data_15m = await self.fetch_ohlcv_data(symbol, '15m', 150)
                if data_15m is None:
                    return None
                
                # Calculate BBW
                bbw_15m = self.bbw_calc.calculate_bbw_exact(data_15m)
                squeeze_15m = self.bbw_calc.detect_squeeze(bbw_15m)
                
                if squeeze_15m['is_squeeze']:
                    # Check deduplication
                    alert_key = f"{coin['symbol']}_bbw"
                    if not self.should_send_alert(alert_key):
                        return None
                    
                    return {
                        'symbol': coin['symbol'],
                        'name': coin.get('name', coin['symbol']),
                        'price': coin.get('current_price', 0),
                        'price_change_24h': coin.get('price_change_percentage_24h', 0),
                        'squeeze_15m': squeeze_15m,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                
                return None
                
            except Exception as e:
                logger.debug(f"❌ Error analyzing {coin.get('symbol', 'Unknown')}: {e}")
                return None
    
    def should_send_alert(self, alert_key: str, cooldown_minutes: int = 60) -> bool:
        """Simple deduplication"""
        now = datetime.utcnow()
        
        if alert_key in self.alert_cache:
            last_sent = self.alert_cache[alert_key]
            time_diff = now - last_sent
            
            if time_diff < timedelta(minutes=cooldown_minutes):
                return False
        
        self.alert_cache[alert_key] = now
        return True
    
    async def send_telegram_alerts(self, alerts: List[Dict]):
        """Send Telegram alerts"""
        if not alerts or not self.bot_token or not self.chat_id:
            return
            
        try:
            message = self.format_alerts(alerts)
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"✅ Telegram alert sent for {len(alerts)} BBW squeezes")
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Telegram error: {error_text}")
                        
        except Exception as e:
            logger.error(f"❌ Failed to send Telegram alert: {e}")
    
    def format_alerts(self, alerts: List[Dict]) -> str:
        """Format alerts message"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M IST')
        
        message = f"🔥 BBW 15M SQUEEZE ALERTS ({len(alerts)} SIGNALS)\n"
        message += f"📅 {timestamp}\n"
        message += f"⚡ BBW touching lowest contraction\n\n"
        
        for i, alert in enumerate(alerts, 1):
            symbol = alert['symbol']
            price = alert['price']
            change_24h = alert.get('price_change_24h', 0)
            squeeze = alert['squeeze_15m']
            
            message += f"{i}. {symbol}/USDT\n"
            message += f"💰 ${price:.6f} ({change_24h:+.1f}%)\n"
            message += f"📊 BBW: {squeeze['bbw_value']}\n"
            message += f"📉 LC: {squeeze['lowest_contraction']}\n"
            message += f"📈 https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval=15\n\n"
        
        message += f"📊 Total Squeezes: {len(alerts)}\n"
        message += f"⏰ Next scan: 15 minutes"
        
        return message
    
    async def run_analysis(self):
        """Main analysis execution"""
        start_time = time.time()
        logger.info("🚀 Starting BBW 15-minute analysis")
        
        # Load market data
        market_data = await self.load_market_data()
        if not market_data:
            logger.error("❌ No market data available - run daily fetcher first")
            return
        
        # Filter coins
        filtered_coins = [
            coin for coin in market_data 
            if coin['symbol'].upper() not in self.blocked_coins
            and coin.get('market_cap', 0) >= 50_000_000
            and coin.get('total_volume', 0) >= 10_000_000
        ]
        
        logger.info(f"📊 Analyzing {len(filtered_coins)} coins")
        
        # Concurrent analysis
        semaphore = asyncio.Semaphore(10)
        tasks = [self.analyze_single_coin(coin, semaphore) for coin in filtered_coins]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter valid results
        alerts = [r for r in results if r and isinstance(r, dict)]
        
        # Close exchanges
        for exchange in self.exchanges:
            await exchange.close()
        
        # Send alerts
        if alerts:
            await self.send_telegram_alerts(alerts)
            logger.info(f"🎯 Sent {len(alerts)} BBW squeeze alerts")
        else:
            logger.info("😴 No BBW squeezes detected")
        
        execution_time = time.time() - start_time
        logger.info(f"✅ Analysis completed in {execution_time:.1f}s")

async def main():
    """Main entry point"""
    try:
        analyzer = BBWAnalyzer()
        await analyzer.run_analysis()
    except Exception as e:
        logger.error(f"💥 Critical error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
