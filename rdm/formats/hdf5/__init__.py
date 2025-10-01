"""
HDF5 data format implementation for Robot Data Management.

This module provides HDF5-specific data loading, saving, and conversion functionality.
"""

import os
import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

import h5py
import numpy as np

from ..core import BaseDataLoader, BaseDataConverter, FormatType, DataValidationError

logger = logging.getLogger(__name__)


class HDF5Loader(BaseDataLoader):
    """HDF5 data loader implementation."""
    
    def __init__(self, path: Union[str, Path], mode: str = "r", **kwargs):
        """
        Initialize HDF5 loader.
        
        Args:
            path: Path to HDF5 file
            mode: File access mode ('r', 'w', 'a')
            **kwargs: Additional parameters
        """
        self.mode = mode
        super().__init__(path, **kwargs)
    
    def load(self) -> Dict[str, Any]:
        """
        Load data from HDF5 file.
        
        Returns:
            Dictionary containing loaded data
        """
        try:
            with h5py.File(self.path, self.mode) as f:
                data = self._recursively_read_group(f)
            logger.info(f"Successfully loaded HDF5 data from {self.path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load HDF5 data from {self.path}: {e}")
            raise DataValidationError(f"HDF5 loading failed: {e}")
    
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to HDF5 file.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to
        """
        save_path = Path(path) if path else self.path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with h5py.File(save_path, "w") as f:
                self._recursively_write_group(f, data)
            logger.info(f"Successfully saved HDF5 data to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save HDF5 data to {save_path}: {e}")
            raise DataValidationError(f"HDF5 saving failed: {e}")
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate HDF5 data structure.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid
        """
        try:
            # Check if data can be written to HDF5
            with h5py.File(self.path, "w") as f:
                self._recursively_write_group(f, data)
            return True
        except Exception as e:
            logger.warning(f"HDF5 validation failed: {e}")
            return False
    
    def _recursively_read_group(self, group: h5py.Group) -> Dict[str, Any]:
        """
        Recursively read HDF5 group structure.
        
        Args:
            group: HDF5 group to read
            
        Returns:
            Dictionary representation of the group
        """
        data = {}
        
        for key in group.keys():
            item = group[key]
            
            if isinstance(item, h5py.Group):
                # Recursively read subgroup
                data[key] = self._recursively_read_group(item)
            elif isinstance(item, h5py.Dataset):
                # Read dataset
                data[key] = item[...]
            else:
                # Handle other types
                data[key] = item
        
        return data
    
    def _recursively_write_group(self, group: h5py.Group, data: Dict[str, Any]) -> None:
        """
        Recursively write dictionary to HDF5 group.
        
        Args:
            group: HDF5 group to write to
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
                
                # Handle object arrays (strings, mixed types)
                if value.dtype == object:
                    # Convert object arrays to string arrays for HDF5 compatibility
                    string_data = []
                    for item in value.flat:
                        if isinstance(item, (str, bytes)):
                            string_data.append(str(item))
                        else:
                            string_data.append(str(item))
                    value = np.array(string_data, dtype="S")
                
                # Create dataset
                try:
                    group.create_dataset(key, data=value, compression="gzip", compression_opts=9)
                except Exception as e:
                    logger.warning(f"Failed to compress {key}, saving without compression: {e}")
                    group.create_dataset(key, data=value)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get HDF5 file metadata.
        
        Returns:
            Dictionary containing metadata
        """
        metadata = super().get_metadata()
        
        try:
            with h5py.File(self.path, "r") as f:
                metadata.update({
                    "hdf5_version": h5py.version.hdf5_version,
                    "groups": list(f.keys()),
                    "file_size": os.path.getsize(self.path),
                })
        except Exception as e:
            logger.warning(f"Failed to get HDF5 metadata: {e}")
        
        return metadata


class HDF5Converter(BaseDataConverter):
    """HDF5 data converter implementation."""
    
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format to HDF5-compatible format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        data = source_loader.load()
        
        # HDF5-specific conversion logic can be added here
        # For now, return data as-is
        return data
    
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert HDF5 data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        # Save data using target loader
        target_loader.save(data)
