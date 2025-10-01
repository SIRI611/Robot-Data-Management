"""
Configuration management for Robot Data Management.

This module provides configuration loading and management functionality.
"""

import json
import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class Config:
    """Configuration management class."""
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.config_path = Path(config_path) if config_path else None
        self.config = self._load_default_config()
        
        if self.config_path and self.config_path.exists():
            self.load_config(self.config_path)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """
        Load default configuration.
        
        Returns:
            Default configuration dictionary
        """
        return {
            "formats": {
                "hdf5": {
                    "compression": "gzip",
                    "compression_opts": 9,
                    "chunk_size": None,
                },
                "zarr": {
                    "compression": "blosc",
                    "chunk_size": "auto",
                },
                "json": {
                    "indent": 2,
                    "ensure_ascii": False,
                },
                "pickle": {
                    "protocol": "highest",
                },
                "numpy": {
                    "allow_pickle": True,
                },
            },
            "validation": {
                "strict": True,
                "check_schema": True,
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
            "conversion": {
                "parallel": True,
                "num_workers": None,
                "batch_size": 1,
            },
        }
    
    def load_config(self, config_path: Union[str, Path]) -> None:
        """
        Load configuration from file.
        
        Args:
            config_path: Path to configuration file
        """
        config_path = Path(config_path)
        
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
            
            # Merge with default config
            self.config = self._merge_configs(self.config, user_config)
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load configuration from {config_path}: {e}")
    
    def save_config(self, config_path: Optional[Union[str, Path]] = None) -> None:
        """
        Save configuration to file.
        
        Args:
            config_path: Optional path to save configuration
        """
        save_path = Path(config_path) if config_path else self.config_path
        
        if not save_path:
            raise ValueError("No configuration path specified")
        
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(save_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Saved configuration to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save configuration to {save_path}: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key (supports dot notation)
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Value to set
        """
        keys = key.split('.')
        config = self.config
        
        # Navigate to the parent of the target key
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # Set the value
        config[keys[-1]] = value
    
    def _merge_configs(self, default: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge user configuration with default configuration.
        
        Args:
            default: Default configuration
            user: User configuration
            
        Returns:
            Merged configuration
        """
        result = default.copy()
        
        for key, value in user.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result


# Global configuration instance
config = Config()
