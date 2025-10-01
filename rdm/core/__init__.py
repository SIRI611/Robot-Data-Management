"""
Core architecture for Robot Data Management (RDM).

This module defines the base classes and interfaces that all data format
implementations must follow.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Iterator
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class FormatType(Enum):
    """Supported data format types."""
    HDF5 = "hdf5"
    ZARR = "zarr"
    RLDS = "rlds"
    LEROBOT = "lerobot"
    JSON = "json"
    PICKLE = "pickle"
    NUMPY = "numpy"
    AUTO = "auto"


class DataValidationError(Exception):
    """Raised when data validation fails."""
    pass


class ConversionError(Exception):
    """Raised when data conversion fails."""
    pass


class BaseDataLoader(ABC):
    """Abstract base class for all data loaders."""
    
    def __init__(self, path: Union[str, Path], **kwargs):
        """
        Initialize the data loader.
        
        Args:
            path: Path to the data file or directory
            **kwargs: Additional format-specific parameters
        """
        self.path = Path(path)
        self.kwargs = kwargs
        self._validate_path()
    
    @abstractmethod
    def load(self) -> Dict[str, Any]:
        """
        Load data from the source.
        
        Returns:
            Dictionary containing the loaded data
        """
        pass
    
    @abstractmethod
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to the target format.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to (defaults to self.path)
        """
        pass
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate data structure and content.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid, False otherwise
            
        Raises:
            DataValidationError: If validation fails
        """
        pass
    
    def _validate_path(self) -> None:
        """Validate that the path exists and is accessible."""
        if not self.path.exists():
            raise FileNotFoundError(f"Path does not exist: {self.path}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the data source.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "path": str(self.path),
            "format": self.__class__.__name__,
            "size": self.path.stat().st_size if self.path.is_file() else None,
        }


class BaseDataConverter(ABC):
    """Abstract base class for data format converters."""
    
    @abstractmethod
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        pass
    
    @abstractmethod
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        pass


class DataManager:
    """Main data management class that orchestrates all operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the data manager.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self._loaders = {}
        self._converters = {}
        self._register_default_loaders()
        self._register_default_converters()
    
    def _register_default_loaders(self) -> None:
        """Register default data loaders."""
        from ..formats import (
            HDF5Loader, ZarrLoader, RLDSLoader, LeRobotLoader,
            JSONLoader, PickleLoader, NumPyLoader
        )
        
        self.register_loader(FormatType.HDF5, HDF5Loader)
        self.register_loader(FormatType.ZARR, ZarrLoader)
        self.register_loader(FormatType.RLDS, RLDSLoader)
        self.register_loader(FormatType.LEROBOT, LeRobotLoader)
        self.register_loader(FormatType.JSON, JSONLoader)
        self.register_loader(FormatType.PICKLE, PickleLoader)
        self.register_loader(FormatType.NUMPY, NumPyLoader)
    
    def _register_default_converters(self) -> None:
        """Register default data converters."""
        from ..formats import (
            HDF5Converter, ZarrConverter, RLDSConverter, LeRobotConverter,
            JSONConverter, PickleConverter, NumPyConverter
        )
        
        # Register converters for all format combinations
        formats = [
            (FormatType.HDF5, HDF5Converter),
            (FormatType.ZARR, ZarrConverter),
            (FormatType.RLDS, RLDSConverter),
            (FormatType.LEROBOT, LeRobotConverter),
            (FormatType.JSON, JSONConverter),
            (FormatType.PICKLE, PickleConverter),
            (FormatType.NUMPY, NumPyConverter),
        ]
        
        for source_format, converter_class in formats:
            for target_format, _ in formats:
                if source_format != target_format:
                    self.register_converter(source_format, target_format, converter_class)
    
    def register_loader(self, format_type: FormatType, loader_class: type) -> None:
        """
        Register a data loader for a specific format.
        
        Args:
            format_type: Format type to register
            loader_class: Loader class to register
        """
        self._loaders[format_type] = loader_class
    
    def register_converter(self, source_format: FormatType, target_format: FormatType, 
                          converter_class: type) -> None:
        """
        Register a data converter between two formats.
        
        Args:
            source_format: Source format type
            target_format: Target format type
            converter_class: Converter class to register
        """
        key = (source_format, target_format)
        self._converters[key] = converter_class
    
    def load(self, path: Union[str, Path], format_type: FormatType = FormatType.AUTO, 
             **kwargs) -> Dict[str, Any]:
        """
        Load data from a file.
        
        Args:
            path: Path to the data file
            format_type: Format type (auto-detect if AUTO)
            **kwargs: Additional format-specific parameters
            
        Returns:
            Loaded data dictionary
        """
        if format_type == FormatType.AUTO:
            format_type = self._detect_format(path)
        
        loader_class = self._loaders.get(format_type)
        if not loader_class:
            raise ValueError(f"No loader registered for format: {format_type}")
        
        loader = loader_class(path, **kwargs)
        return loader.load()
    
    def save(self, data: Dict[str, Any], path: Union[str, Path], 
             format_type: FormatType, **kwargs) -> None:
        """
        Save data to a file.
        
        Args:
            data: Data dictionary to save
            path: Path to save the data
            format_type: Target format type
            **kwargs: Additional format-specific parameters
        """
        loader_class = self._loaders.get(format_type)
        if not loader_class:
            raise ValueError(f"No loader registered for format: {format_type}")
        
        loader = loader_class(path, **kwargs)
        loader.save(data)
    
    def convert(self, source_path: Union[str, Path], target_path: Union[str, Path],
                source_format: FormatType = FormatType.AUTO, 
                target_format: FormatType = FormatType.AUTO, **kwargs) -> None:
        """
        Convert data between formats.
        
        Args:
            source_path: Path to source data
            target_path: Path for target data
            source_format: Source format type
            target_format: Target format type
            **kwargs: Additional conversion parameters
        """
        if source_format == FormatType.AUTO:
            source_format = self._detect_format(source_path)
        if target_format == FormatType.AUTO:
            target_format = self._detect_format(target_path)
        
        # Load source data
        source_data = self.load(source_path, source_format, **kwargs)
        
        # Save to target format
        self.save(source_data, target_path, target_format, **kwargs)
    
    def batch_convert(self, source_dir: Union[str, Path], target_dir: Union[str, Path],
                     source_format: FormatType = FormatType.AUTO,
                     target_format: FormatType = FormatType.AUTO, **kwargs) -> None:
        """
        Batch convert multiple files.
        
        Args:
            source_dir: Source directory
            target_dir: Target directory
            source_format: Source format type
            target_format: Target format type
            **kwargs: Additional conversion parameters
        """
        source_dir = Path(source_dir)
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Find all files in source directory
        source_files = self._find_files(source_dir, source_format)
        
        for source_file in source_files:
            relative_path = source_file.relative_to(source_dir)
            target_file = target_dir / relative_path.with_suffix(self._get_extension(target_format))
            
            try:
                self.convert(source_file, target_file, source_format, target_format, **kwargs)
                logger.info(f"Converted: {source_file} -> {target_file}")
            except Exception as e:
                logger.error(f"Failed to convert {source_file}: {e}")
    
    def validate(self, path: Union[str, Path], format_type: FormatType = FormatType.AUTO) -> bool:
        """
        Validate data file.
        
        Args:
            path: Path to data file
            format_type: Format type
            
        Returns:
            True if data is valid
        """
        if format_type == FormatType.AUTO:
            format_type = self._detect_format(path)
        
        loader_class = self._loaders.get(format_type)
        if not loader_class:
            raise ValueError(f"No loader registered for format: {format_type}")
        
        loader = loader_class(path)
        data = loader.load()
        return loader.validate(data)
    
    def _detect_format(self, path: Union[str, Path]) -> FormatType:
        """
        Auto-detect format based on file extension.
        
        Args:
            path: Path to file
            
        Returns:
            Detected format type
        """
        path = Path(path)
        suffix = path.suffix.lower()
        
        format_mapping = {
            '.h5': FormatType.HDF5,
            '.hdf5': FormatType.HDF5,
            '.zarr': FormatType.ZARR,
            '.json': FormatType.JSON,
            '.pkl': FormatType.PICKLE,
            '.pickle': FormatType.PICKLE,
            '.npy': FormatType.NUMPY,
            '.npz': FormatType.NUMPY,
        }
        
        return format_mapping.get(suffix, FormatType.HDF5)  # Default to HDF5
    
    def _find_files(self, directory: Path, format_type: FormatType) -> List[Path]:
        """
        Find files of a specific format in a directory.
        
        Args:
            directory: Directory to search
            format_type: Format type to find
            
        Returns:
            List of matching file paths
        """
        extensions = {
            FormatType.HDF5: ['.h5', '.hdf5'],
            FormatType.ZARR: ['.zarr'],
            FormatType.JSON: ['.json'],
            FormatType.PICKLE: ['.pkl', '.pickle'],
            FormatType.NUMPY: ['.npy', '.npz'],
        }
        
        ext_list = extensions.get(format_type, [])
        files = []
        
        for ext in ext_list:
            files.extend(directory.rglob(f"*{ext}"))
        
        return files
    
    def _get_extension(self, format_type: FormatType) -> str:
        """
        Get file extension for a format type.
        
        Args:
            format_type: Format type
            
        Returns:
            File extension
        """
        extension_mapping = {
            FormatType.HDF5: '.h5',
            FormatType.ZARR: '.zarr',
            FormatType.JSON: '.json',
            FormatType.PICKLE: '.pkl',
            FormatType.NUMPY: '.npy',
        }
        
        return extension_mapping.get(format_type, '.h5')
