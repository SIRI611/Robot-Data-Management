"""
Zarr data format implementation for Robot Data Management.

This module provides Zarr-specific data loading, saving, and conversion functionality.
"""

import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

import zarr
import numpy as np

from ..core import BaseDataLoader, BaseDataConverter, FormatType, DataValidationError

logger = logging.getLogger(__name__)


class ZarrLoader(BaseDataLoader):
    """Zarr data loader implementation."""
    
    def __init__(self, path: Union[str, Path], mode: str = "r", **kwargs):
        """
        Initialize Zarr loader.
        
        Args:
            path: Path to Zarr store
            mode: Store access mode ('r', 'w', 'a')
            **kwargs: Additional parameters
        """
        self.mode = mode
        super().__init__(path, **kwargs)
    
    def load(self) -> Dict[str, Any]:
        """
        Load data from Zarr store.
        
        Returns:
            Dictionary containing loaded data
        """
        try:
            store = zarr.open(str(self.path), mode=self.mode)
            data = self._recursively_read_group(store)
            logger.info(f"Successfully loaded Zarr data from {self.path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load Zarr data from {self.path}: {e}")
            raise DataValidationError(f"Zarr loading failed: {e}")
    
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to Zarr store.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to
        """
        save_path = Path(path) if path else self.path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            store = zarr.open(str(save_path), mode="w")
            self._recursively_write_group(store, data)
            logger.info(f"Successfully saved Zarr data to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save Zarr data to {save_path}: {e}")
            raise DataValidationError(f"Zarr saving failed: {e}")
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate Zarr data structure.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid
        """
        try:
            # Check if data can be written to Zarr
            store = zarr.open(str(self.path), mode="w")
            self._recursively_write_group(store, data)
            return True
        except Exception as e:
            logger.warning(f"Zarr validation failed: {e}")
            return False
    
    def _recursively_read_group(self, group: zarr.Group) -> Dict[str, Any]:
        """
        Recursively read Zarr group structure.
        
        Args:
            group: Zarr group to read
            
        Returns:
            Dictionary representation of the group
        """
        data = {}
        
        for key in group.keys():
            item = group[key]
            
            if isinstance(item, zarr.Group):
                # Recursively read subgroup
                data[key] = self._recursively_read_group(item)
            elif isinstance(item, zarr.Array):
                # Read array
                data[key] = item[...]
            else:
                # Handle other types
                data[key] = item
        
        return data
    
    def _recursively_write_group(self, group: zarr.Group, data: Dict[str, Any]) -> None:
        """
        Recursively write dictionary to Zarr group.
        
        Args:
            group: Zarr group to write to
            data: Data dictionary to write
        """
        for key, value in data.items():
            if isinstance(value, dict):
                # Create subgroup for nested dictionaries
                subgroup = group.create_group(key)
                self._recursively_write_group(subgroup, value)
            else:
                # Convert value to numpy array if needed
                if not isinstance(value, np.ndarray):
                    if isinstance(value, (list, tuple)):
                        value = np.array(value)
                    else:
                        # Single value
                        value = np.array([value])
                
                # Create array with compression
                try:
                    group.create_dataset(key, data=value, compression="blosc")
                except Exception as e:
                    logger.warning(f"Failed to compress {key}, saving without compression: {e}")
                    group.create_dataset(key, data=value)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get Zarr store metadata.
        
        Returns:
            Dictionary containing metadata
        """
        metadata = super().get_metadata()
        
        try:
            store = zarr.open(str(self.path), mode="r")
            metadata.update({
                "zarr_version": zarr.__version__,
                "groups": list(store.keys()),
                "store_type": type(store.store).__name__,
            })
        except Exception as e:
            logger.warning(f"Failed to get Zarr metadata: {e}")
        
        return metadata


class ZarrConverter(BaseDataConverter):
    """Zarr data converter implementation."""
    
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format to Zarr-compatible format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        data = source_loader.load()
        
        # Zarr-specific conversion logic can be added here
        # For now, return data as-is
        return data
    
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert Zarr data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        # Save data using target loader
        target_loader.save(data)
