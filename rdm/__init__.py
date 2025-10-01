"""
Robot Data Management (RDM) - Main Package

A comprehensive library for managing robot data across multiple formats.
"""

from .core import (
    BaseDataLoader,
    BaseDataConverter,
    DataManager,
    FormatType,
    DataValidationError,
    ConversionError,
)

__version__ = "0.1.0"
__author__ = "Robot Data Management Team"

__all__ = [
    "BaseDataLoader",
    "BaseDataConverter", 
    "DataManager",
    "FormatType",
    "DataValidationError",
    "ConversionError",
]
