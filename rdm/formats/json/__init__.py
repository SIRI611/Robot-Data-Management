"""
JSON data format implementation for Robot Data Management.

This module provides JSON-specific data loading, saving, and conversion functionality.
"""

import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

import json
import numpy as np

from ..core import BaseDataLoader, BaseDataConverter, FormatType, DataValidationError

logger = logging.getLogger(__name__)


class JSONLoader(BaseDataLoader):
    """JSON data loader implementation."""
    
    def load(self) -> Dict[str, Any]:
        """
        Load data from JSON file.
        
        Returns:
            Dictionary containing loaded data
        """
        try:
            with open(self.path, 'r') as f:
                data = json.load(f)
            logger.info(f"Successfully loaded JSON data from {self.path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load JSON data from {self.path}: {e}")
            raise DataValidationError(f"JSON loading failed: {e}")
    
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to JSON file.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to
        """
        save_path = Path(path) if path else self.path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(save_path, 'w') as f:
                json.dump(data, f, indent=2, default=self._json_serializer)
            logger.info(f"Successfully saved JSON data to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save JSON data to {save_path}: {e}")
            raise DataValidationError(f"JSON saving failed: {e}")
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate JSON data structure.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid
        """
        try:
            # Check if data can be serialized to JSON
            json.dumps(data, default=self._json_serializer)
            return True
        except Exception as e:
            logger.warning(f"JSON validation failed: {e}")
            return False
    
    def _json_serializer(self, obj):
        """
        Custom JSON serializer for numpy arrays and other types.
        
        Args:
            obj: Object to serialize
            
        Returns:
            JSON-serializable object
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        else:
            return str(obj)


class JSONConverter(BaseDataConverter):
    """JSON data converter implementation."""
    
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format to JSON-compatible format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        data = source_loader.load()
        
        # JSON-specific conversion logic can be added here
        # For now, return data as-is
        return data
    
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert JSON data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        # Save data using target loader
        target_loader.save(data)
