#!/usr/bin/env python3
"""
BBW 15-Minute Alert System - Main Analyzer
Exact Pine Script BBW calculation with concurrent processing
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import pandas as pd
import ccxt.async_support as ccxt

from utils.logger import setup_logger
from utils.performance_monitor import PerformanceMonitor
from indicators.bbw_calculator import BBWCalculator
from alerts.telegram_sender import TelegramSender
from alerts.deduplication import AlertDeduplicator

logger = setup_logger(__name__)
perf_monitor = PerformanceMonitor()

class BBWAnalyzer:
    def __init__(self):
        self.config = self._load_config()
        self.bbw_calc = BBWCalculator()
        self.telegram = TelegramSender()
        self.deduplicator = AlertDeduplicator()
        
        # Initialize exchanges with India-friendly options
        self.exchanges = self._setup_exchanges()
        
    def _load_config(self) -> Dict:
        """Load system configuration"""
        with open('config/config.yaml', 'r') as f:
            import yaml
            return yaml.safe_load(f)
    
    def _setup_exchanges(self) -> List:
        """Setup exchange connections with fallback options"""
        exchanges = []
        
        # Primary: BingX
        if os.getenv('BINGX_API_KEY'):
            exchanges.append(ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY'),
                'secret': os.getenv('BINGX_SECRET_KEY'),
                'sandbox': False,
                'rateLimit': 1200,
                'enableRateLimit': True,
            }))
        
        # Fallback: KuCoin
        exchanges.append(ccxt.kucoin({
            'rateLimit': 1200,
            'enableRateLimit': True,
        }))
        
        # Fallback: WazirX (India-friendly)
        exchanges.append(ccxt.wazirx({
            'rateLimit': 1200,
            'enableRateLimit': True,
        }))
        
        return exchanges
    
    async def load_market_data(self) -> Optional[List[Dict]]:
        """Load cached market data from daily fetcher"""
        try:
            cache_file = 'cache/high_risk_market_data.json'
            if not os.path.exists(cache_file):
                logger.error("❌ Market data cache not found. Skipping analysis.")
                return None
                
            with open(cache_file, 'r') as f:
                data = json.load(f)
                
            logger.info(f"✅ Loaded {len(data)} coins from cache")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error loading market data: {e}")
            return None
    
    def load_blocked_coins(self) -> set:
        """Load blocked coins list"""
        try:
            with open('config/blocked_coins.txt', 'r') as f:
                blocked = {line.strip().upper() for line in f if line.strip()}
            logger.info(f"📋 Loaded {len(blocked)} blocked coins")
            return blocked
        except Exception as e:
            logger.warning(f"⚠️ Could not load blocked coins: {e}")
            return set()
    
    async def fetch_ohlcv_data(self, symbol: str, timeframe: str = '15m', limit: int = 150) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data with exchange fallback"""
        for exchange in self.exchanges:
            try:
                # Check if symbol exists on exchange
                await exchange.load_markets()
                if symbol not in exchange.markets:
                    continue
                
                # Fetch OHLCV data
                ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                
                if len(ohlcv) < 125:  # Minimum required for BBW calculation
                    logger.warning(f"⚠️ {symbol}: Insufficient data ({len(ohlcv)} candles)")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                logger.debug(f"✅ {symbol}: Fetched {len(df)} candles from {exchange.name}")
                return df
                
            except Exception as e:
                logger.debug(f"❌ {symbol} failed on {exchange.name}: {e}")
                continue
        
        logger.warning(f"❌ {symbol}: Failed on all exchanges")
        return None
    
    async def analyze_single_coin(self, coin: Dict, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Analyze single coin with BBW calculation"""
        async with semaphore:
            try:
                symbol = f"{coin['symbol'].upper()}/USDT"
                
                # Fetch data for both timeframes
                data_15m = await self.fetch_ohlcv_data(symbol, '15m', 150)
                data_1h = await self.fetch_ohlcv_data(symbol, '1h', 150)
                
                if data_15m is None or data_1h is None:
                    return None
                
                # Calculate BBW for both timeframes
                bbw_15m = self.bbw_calc.calculate_bbw_exact(data_15m)
                bbw_1h = self.bbw_calc.calculate_bbw_exact(data_1h)
                
                # Detect squeezes
                squeeze_15m = self.bbw_calc.detect_squeeze(bbw_15m)
                squeeze_1h = self.bbw_calc.detect_squeeze(bbw_1h)
                
                # Check if any squeeze detected
                if squeeze_15m['is_squeeze'] or squeeze_1h['is_squeeze']:
                    
                    # Check deduplication
                    alert_key = f"{coin['symbol']}_bbw"
                    if not self.deduplicator.should_send_alert(alert_key):
                        logger.debug(f"🔄 {coin['symbol']}: Alert deduplicated")
                        return None
                    
                    # Create alert data
                    alert_data = {
                        'symbol': coin['symbol'],
                        'name': coin['name'],
                        'price': coin['current_price'],
                        'price_change_24h': coin.get('price_change_percentage_24h', 0),
                        'market_cap': coin['market_cap'],
                        'volume_24h': coin['total_volume'],
                        'squeeze_15m': squeeze_15m,
                        'squeeze_1h': squeeze_1h,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    
                    logger.info(f"🎯 {coin['symbol']}: BBW Squeeze detected!")
                    return alert_data
                
                return None
                
            except Exception as e:
                logger.error(f"❌ Error analyzing {coin.get('symbol', 'Unknown')}: {e}")
                return None
    
    async def run_analysis(self):
        """Main analysis execution with concurrent processing"""
        start_time = time.time()
        logger.info("🚀 Starting BBW 15-minute analysis")
        
        # Load market data
        market_data = await self.load_market_data()
        if not market_data:
            return
        
        # Load blocked coins
        blocked_coins = self.load_blocked_coins()
        
        # Filter coins
        filtered_coins = [
            coin for coin in market_data 
            if coin['symbol'].upper() not in blocked_coins
            and coin['market_cap'] >= 50_000_000  # $50M minimum
            and coin['total_volume'] >= 10_000_000  # $10M minimum
        ]
        
        logger.info(f"📊 Analyzing {len(filtered_coins)} coins (filtered from {len(market_data)})")
        
        # Concurrent analysis with semaphore for rate limiting
        semaphore = asyncio.Semaphore(10)  # Max 10 concurrent requests
        tasks = [self.analyze_single_coin(coin, semaphore) for coin in filtered_coins]
        
        # Execute with progress tracking
        results = []
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            
            if result:
                results.append(result)
            
            # Log progress every 50 coins
            if completed % 50 == 0:
                logger.info(f"📈 Progress: {completed}/{len(filtered_coins)} coins analyzed")
        
        # Close exchange connections
        for exchange in self.exchanges:
            await exchange.close()
        
        # Send alerts if any found
        if results:
            await self.telegram.send_batch_alerts(results)
            logger.info(f"🎯 Sent {len(results)} BBW squeeze alerts")
        else:
            logger.info("😴 No BBW squeezes detected")
        
        # Log performance metrics
        execution_time = time.time() - start_time
        perf_monitor.log_analysis_run(len(filtered_coins), len(results), execution_time)
        
        logger.info(f"✅ Analysis completed in {execution_time:.1f}s")

async def main():
    """Main entry point"""
    try:
        analyzer = BBWAnalyzer()
        await analyzer.run_analysis()
    except Exception as e:
        logger.error(f"💥 Critical error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
