"""
Unified Error Handling Strategy
"""
import time
from functools import wraps
from typing import Callable, Any, Optional
from src.utils.logger import trading_logger

class TradingError(Exception):
    """Base exception for trading system"""
    pass

class ExchangeError(TradingError):
    """Exchange API related errors"""
    pass

class DataError(TradingError):
    """Data processing related errors"""
    pass

class IndicatorError(TradingError):
    """Indicator calculation related errors"""
    pass

def with_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Retry decorator with exponential backoff
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = trading_logger.get_logger('ErrorHandler')
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Final attempt failed for {func.__name__}: {str(e)}")
                        raise
                    
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            
            return None
        return wrapper
    return decorator

def handle_errors(error_type: type = Exception, return_value: Any = None, log_error: bool = True):
    """
    Error handling decorator with custom return values
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except error_type as e:
                if log_error:
                    logger = trading_logger.get_logger('ErrorHandler')
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                return return_value
        return wrapper
    return decorator
