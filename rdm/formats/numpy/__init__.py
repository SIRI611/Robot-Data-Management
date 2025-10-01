"""
NumPy data format implementation for Robot Data Management.

This module provides NumPy-specific data loading, saving, and conversion functionality.
"""

import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

import numpy as np

from ..core import BaseDataLoader, BaseDataConverter, FormatType, DataValidationError

logger = logging.getLogger(__name__)


class NumPyLoader(BaseDataLoader):
    """NumPy data loader implementation."""
    
    def load(self) -> Dict[str, Any]:
        """
        Load data from NumPy file.
        
        Returns:
            Dictionary containing loaded data
        """
        try:
            if self.path.suffix == '.npz':
                # Load compressed NumPy file
                data = dict(np.load(self.path))
            else:
                # Load single array
                array = np.load(self.path)
                data = {"data": array}
            
            logger.info(f"Successfully loaded NumPy data from {self.path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load NumPy data from {self.path}: {e}")
            raise DataValidationError(f"NumPy loading failed: {e}")
    
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to NumPy file.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to
        """
        save_path = Path(path) if path else self.path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if save_path.suffix == '.npz':
                # Save as compressed NumPy file
                np.savez_compressed(save_path, **data)
            else:
                # Save single array
                if len(data) == 1:
                    array = list(data.values())[0]
                    np.save(save_path, array)
                else:
                    # Multiple arrays, save as compressed
                    np.savez_compressed(save_path.with_suffix('.npz'), **data)
            
            logger.info(f"Successfully saved NumPy data to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save NumPy data to {save_path}: {e}")
            raise DataValidationError(f"NumPy saving failed: {e}")
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate NumPy data structure.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid
        """
        try:
            # Check if all values are numpy arrays or can be converted
            for key, value in data.items():
                if not isinstance(value, np.ndarray):
                    # Try to convert to numpy array
                    np.array(value)
            return True
        except Exception as e:
            logger.warning(f"NumPy validation failed: {e}")
            return False


class NumPyConverter(BaseDataConverter):
    """NumPy data converter implementation."""
    
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format to NumPy-compatible format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        data = source_loader.load()
        
        # Convert all values to numpy arrays
        numpy_data = {}
        for key, value in data.items():
            if not isinstance(value, np.ndarray):
                numpy_data[key] = np.array(value)
            else:
                numpy_data[key] = value
        
        return numpy_data
    
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert NumPy data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        # Save data using target loader
        target_loader.save(data)
