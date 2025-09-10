#!/usr/bin/env python3
"""
Logging configuration for BBW Alert System
"""
import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Setup logger with file and console output
    
    Args:
        name: Logger name
        log_level: Logging level
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Create cache directory for logs
    cache_dir = Path(__file__).parent.parent.parent / 'cache'
    cache_dir.mkdir(exist_ok=True)
    
    # File handler
    log_file = cache_dir / 'bbw_analysis.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
