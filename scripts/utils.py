#!/usr/bin/env python3
"""
Utility functions for GitHub Analytics Tracker.
Common helper functions used across the application.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Optional


def setup_logging(level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path. If None, logs to console only.
    
    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_current_utc_timestamp() -> str:
    """Get current UTC timestamp in ISO format.
    
    Returns:
        ISO formatted UTC timestamp string
    """
    return datetime.now(timezone.utc).isoformat()


def format_timestamp(dt: datetime) -> str:
    """Format datetime object as ISO string.
    
    Args:
        dt: Datetime object to format
    
    Returns:
        ISO formatted timestamp string
    """
    if dt.tzinfo is None:
        # Assume UTC if no timezone info
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_repository_string(repo_string: str) -> tuple[str, str]:
    """Parse repository string in 'owner/name' format.
    
    Args:
        repo_string: Repository string in 'owner/name' format
    
    Returns:
        Tuple of (owner, name)
    
    Raises:
        ValueError: If repository string format is invalid
    """
    if '/' not in repo_string:
        raise ValueError(f"Invalid repository format: {repo_string}. Expected 'owner/name'")
    
    parts = repo_string.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid repository format: {repo_string}. Expected 'owner/name'")
    
    return parts[0], parts[1]


def safe_get(dictionary: dict, key: str, default=None):
    """Safely get value from dictionary with optional default.
    
    Args:
        dictionary: Dictionary to get value from
        key: Key to look up
        default: Default value if key not found
    
    Returns:
        Value from dictionary or default
    """
    return dictionary.get(key, default)


def truncate_string(text: str, max_length: int = 100) -> str:
    """Truncate string to maximum length with ellipsis.
    
    Args:
        text: Text to truncate
        max_length: Maximum length before truncation
    
    Returns:
        Truncated string with ellipsis if needed
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."