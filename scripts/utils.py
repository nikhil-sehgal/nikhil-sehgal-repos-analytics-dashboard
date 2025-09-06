#!/usr/bin/env python3
"""
Utility functions for GitHub Analytics Tracker.
Common helper functions used across the application.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path


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

def safe_json_load(file_path: str) -> Dict[str, Any]:
    """Safely load JSON from file.
    
    Args:
        file_path: Path to JSON file
    
    Returns:
        Dictionary from JSON file, or empty dict if file doesn't exist or is invalid
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Failed to load JSON from {file_path}: {e}")
    
    return {}


def safe_json_save(data: Dict[str, Any], file_path: str) -> bool:
    """Safely save data to JSON file.
    
    Args:
        data: Data to save
        file_path: Path to save JSON file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure directory exists
        ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except (IOError, TypeError) as e:
        logging.error(f"Failed to save JSON to {file_path}: {e}")
        return False


def ensure_directory(directory_path: str) -> bool:
    """Ensure directory exists, create if it doesn't.
    
    Args:
        directory_path: Path to directory
    
    Returns:
        True if directory exists or was created successfully
    """
    try:
        if directory_path:  # Only create if path is not empty
            Path(directory_path).mkdir(parents=True, exist_ok=True)
        return True
    except OSError as e:
        logging.error(f"Failed to create directory {directory_path}: {e}")
        return False


def format_date_for_data_key(date: datetime) -> str:
    """Format date for use as data key.
    
    Args:
        date: Date to format
    
    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    return date.strftime('%Y-%m-%d')


def format_month_for_data_key(date: datetime) -> str:
    """Format date for monthly data key.
    
    Args:
        date: Date to format
    
    Returns:
        Formatted month string (YYYY-MM)
    """
    return date.strftime('%Y-%m')


class DataFileManager:
    """Manages data file operations and paths."""
    
    def __init__(self, base_path: str = 'data'):
        """Initialize data file manager.
        
        Args:
            base_path: Base directory for data files
        """
        self.base_path = base_path
        ensure_directory(base_path)
    
    def get_repository_data_path(self, owner: str, name: str) -> str:
        """Get path for repository data directory.
        
        Args:
            owner: Repository owner
            name: Repository name
        
        Returns:
            Path to repository data directory
        """
        repo_path = os.path.join(self.base_path, owner, name)
        ensure_directory(repo_path)
        return repo_path
    
    def get_daily_metrics_path(self, owner: str, name: str) -> str:
        """Get path for daily metrics file.
        
        Args:
            owner: Repository owner
            name: Repository name
        
        Returns:
            Path to daily metrics JSON file
        """
        repo_path = self.get_repository_data_path(owner, name)
        return os.path.join(repo_path, 'daily_metrics.json')
    
    def get_monthly_summary_path(self, owner: str, name: str) -> str:
        """Get path for monthly summary file.
        
        Args:
            owner: Repository owner
            name: Repository name
        
        Returns:
            Path to monthly summary JSON file
        """
        repo_path = self.get_repository_data_path(owner, name)
        return os.path.join(repo_path, 'monthly_summary.json')
    
    def get_repository_info_path(self, owner: str, name: str) -> str:
        """Get path for repository info file.
        
        Args:
            owner: Repository owner
            name: Repository name
        
        Returns:
            Path to repository info JSON file
        """
        repo_path = self.get_repository_data_path(owner, name)
        return os.path.join(repo_path, 'repository_info.json') 
   
    def load_daily_data(self, owner: str, name: str, year: int) -> Dict[str, Any]:
        """Load daily data for a repository and year.
        
        Args:
            owner: Repository owner
            name: Repository name
            year: Year to load data for
        
        Returns:
            Dictionary containing daily data for the year
        """
        daily_metrics_path = self.get_daily_metrics_path(owner, name)
        daily_data = safe_json_load(daily_metrics_path)
        
        # Return data for the specific year, or empty dict if not found
        return daily_data.get(str(year), {})