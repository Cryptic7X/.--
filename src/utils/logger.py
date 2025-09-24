"""
Centralized Logging System for Multi-Indicator Trading
"""
import logging
import os
from datetime import datetime

class TradingLogger:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.setup_logging()
        return cls._instance
    
    def setup_logging(self):
        """Setup structured logging with different levels"""
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_level),
            handlers=[console_handler],
            format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s'
        )
        
        self.logger = logging.getLogger('TradingSystem')
    
    def get_logger(self, name: str = None):
        """Get logger instance for specific component"""
        if name:
            return logging.getLogger(f'TradingSystem.{name}')
        return self.logger

# Global logger instance
trading_logger = TradingLogger()
