"""
Base Analyzer Class - Code Deduplication
"""
import concurrent.futures
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from src.utils.logger import trading_logger
from src.utils.dataset_loader import dataset_loader
from src.utils.error_handler import with_retry, handle_errors
from src.exchanges.simple_exchange import SimpleExchangeManager

class BaseAnalyzer(ABC):
    """
    Base class for all analyzers - eliminates code duplication
    """
    
    def __init__(self, config: Dict, system_name: str):
        self.config = config
        self.system_name = system_name
        self.system_config = config[f'{system_name}_config']
        self.logger = trading_logger.get_logger(f'{system_name.upper()}Analyzer')
        self.exchange_manager = SimpleExchangeManager()
        
        self.timeframe = self.system_config['timeframe']
        self.max_workers = self.system_config['processing']['max_workers']
        self.timeout = self.system_config['processing']['timeout_seconds']
        
    def load_dataset(self) -> List[Dict]:
        """Load filtered dataset using shared loader"""
        return dataset_loader.load_dataset(
            self.system_config['cache_file'],
            self.system_config['filters']
        )
    
    @handle_errors(return_value=None)
    @with_retry(max_attempts=3)
    def analyze_single_coin(self, coin_data: Dict) -> Optional[Dict]:
        """
        Template method for single coin analysis
        """
        symbol = coin_data['symbol']
        
        try:
            # Fetch OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, self.timeframe, limit=self.config['shared']['data_limit']
            )
            
            if not ohlcv_data:
                self.logger.debug(f"No OHLCV data for {symbol}")
                return None
            
            # System-specific analysis (implemented by child classes)
            result = self._analyze_indicator(ohlcv_data, coin_data, exchange_used)
            
            if result:
                self.logger.info(f"Signal detected: {symbol} ({result.get('signal_type', 'UNKNOWN')})")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Analysis failed for {symbol}: {str(e)}")
            return None
    
    @abstractmethod
    def _analyze_indicator(self, ohlcv_data: Dict, coin_data: Dict, exchange_used: str) -> Optional[Dict]:
        """
        System-specific indicator analysis - must be implemented by child classes
        """
        pass
    
    def process_coins_parallel(self, coins: List[Dict]) -> List[Dict]:
        """
        Standardized parallel processing
        """
        signals = []
        
        self.logger.info(f"Processing {len(coins)} coins with {self.max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_coin = {
                executor.submit(self.analyze_single_coin, coin): coin 
                for coin in coins
            }
            
            processed = 0
            for future in concurrent.futures.as_completed(future_to_coin, timeout=self.timeout):
                coin = future_to_coin[future]
                processed += 1
                
                try:
                    result = future.result(timeout=5)
                    if result:
                        signals.append(result)
                    
                    if processed % 25 == 0:
                        self.logger.info(f"Progress: {processed}/{len(coins)} coins processed")
                        
                except Exception as e:
                    self.logger.warning(f"Error processing {coin['symbol']}: {str(e)}")
                    continue
        
        return signals
    
    def run_analysis(self):
        """
        Main analysis workflow - standardized across all systems
        """
        self.logger.info(f"{self.system_name.upper()} {self.timeframe.upper()} ANALYSIS STARTING")
        self.logger.info("=" * 50)
        
        start_time = datetime.utcnow()
        
        # Load dataset
        coins = self.load_dataset()
        if not coins:
            self.logger.error("No coins loaded - analysis aborted")
            return
        
        self.logger.info(f"Analyzing {len(coins)} coins on {self.timeframe} timeframe")
        
        # Process coins
        signals = self.process_coins_parallel(coins)
        
        # Send alerts (implemented by child classes)
        if signals:
            success = self._send_alerts(signals)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            self.logger.info("=" * 50)
            self.logger.info(f"{self.system_name.upper()} ANALYSIS COMPLETE")
            self.logger.info(f"Signals Found: {len(signals)}")
            self.logger.info(f"Alert Sent: {'Yes' if success else 'Failed'}")
            self.logger.info(f"Processing Time: {processing_time:.1f}s")
            self.logger.info(f"Coins Processed: {len(coins)}")
            self.logger.info("=" * 50)
        else:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            self.logger.info(f"No {self.system_name} signals found in this cycle")
            self.logger.info(f"Processing Time: {processing_time:.1f}s")
            self.logger.info(f"Coins Analyzed: {len(coins)}")
    
    @abstractmethod
    def _send_alerts(self, signals: List[Dict]) -> bool:
        """
        Send alerts - must be implemented by child classes
        """
        pass
