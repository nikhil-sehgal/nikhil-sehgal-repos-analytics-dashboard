#!/usr/bin/env python3
"""
Configuration management for GitHub Analytics Tracker.
Handles loading and validation of configuration settings.
"""

import json
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class Repository:
    """Represents a repository configuration."""
    owner: str
    name: str
    enabled: bool = True
    
    @property
    def full_name(self) -> str:
        """Returns the full repository name (owner/name)."""
        return f"{self.owner}/{self.name}"
    
    def __str__(self) -> str:
        return self.full_name


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file. If None, looks for config.json
                        in parent directory.
        """
        if config_path is None:
            # Look for config.json in parent directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(os.path.dirname(script_dir), 'config.json')
        
        self.config_path = config_path
        self._config = None
        self._repositories = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file.
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        if self._config is None:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
        
        return self._config
    
    def get_repositories(self) -> List[Repository]:
        """Get list of repositories to track.
        
        Returns:
            List of Repository objects
        """
        if self._repositories is None:
            config = self.load_config()
            repos_config = config.get('repositories', [])
            
            self._repositories = []
            for repo_config in repos_config:
                if isinstance(repo_config, str):
                    # Handle "owner/name" format
                    if '/' in repo_config:
                        owner, name = repo_config.split('/', 1)
                        self._repositories.append(Repository(owner=owner, name=name, enabled=True))
                elif isinstance(repo_config, dict):
                    # Handle {"owner": "...", "name": "...", "enabled": ...} format
                    owner = repo_config.get('owner')
                    name = repo_config.get('name')
                    enabled = repo_config.get('enabled', True)
                    if owner and name:
                        self._repositories.append(Repository(owner=owner, name=name, enabled=enabled))
        
        return self._repositories
    
    def get_github_token(self) -> Optional[str]:
        """Get GitHub token from environment or config.
        
        Returns:
            GitHub token or None if not found
        """
        # First try environment variable
        token = os.getenv('GITHUB_TOKEN')
        if token:
            return token
        
        # Then try config file
        config = self.load_config()
        return config.get('github_token')
    
    def get_data_storage_config(self) -> Dict[str, Any]:
        """Get data storage configuration.
        
        Returns:
            Data storage configuration dictionary
        """
        config = self.load_config()
        return config.get('data_storage', {})
    
    def get_collection_config(self) -> Dict[str, Any]:
        """Get data collection configuration.
        
        Returns:
            Collection configuration dictionary
        """
        config = self.load_config()
        return config.get('collection', {})
    
    def validate_config(self) -> bool:
        """Validate configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        config = self.load_config()
        
        # Check required fields
        if not config.get('repositories'):
            raise ValueError("No repositories configured")
        
        # Validate repositories
        repositories = self.get_repositories()
        if not repositories:
            raise ValueError("No valid repositories found in configuration")
        
        # Check GitHub token
        token = self.get_github_token()
        if not token:
            raise ValueError("GitHub token not found in environment or config")
        
        return True