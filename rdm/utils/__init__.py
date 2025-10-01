"""
Utility functions for Robot Data Management.

This module provides common utility functions used across different data formats.
"""

import logging
from typing import Any, Dict, List, Union
from pathlib import Path
import hashlib
import json

logger = logging.getLogger(__name__)


def flatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        data: Dictionary to flatten
        separator: Separator to use for nested keys
        
    Returns:
        Flattened dictionary
    """
    def _flatten(obj, parent_key="", sep="."):
        items = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((parent_key, obj))
        return dict(items)
    
    return _flatten(data, sep=separator)


def unflatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """
    Unflatten a dictionary.
    
    Args:
        data: Flattened dictionary
        separator: Separator used in flattened keys
        
    Returns:
        Nested dictionary
    """
    result = {}
    for key, value in data.items():
        keys = key.split(separator)
        current = result
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value
    return result


def calculate_file_hash(file_path: Union[str, Path], algorithm: str = "sha256") -> str:
    """
    Calculate hash of a file.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm to use
        
    Returns:
        Hexadecimal hash string
    """
    hash_obj = hashlib.new(algorithm)
    
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    
    return hash_obj.hexdigest()


def validate_data_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
    """
    Validate data against a schema.
    
    Args:
        data: Data dictionary to validate
        schema: Schema dictionary
        
    Returns:
        True if data matches schema
    """
    try:
        # Simple schema validation - can be extended with more sophisticated validation
        for key, expected_type in schema.items():
            if key not in data:
                logger.warning(f"Missing required key: {key}")
                return False
            
            if not isinstance(data[key], expected_type):
                logger.warning(f"Type mismatch for key {key}: expected {expected_type}, got {type(data[key])}")
                return False
        
        return True
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return False


def get_data_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get information about data structure.
    
    Args:
        data: Data dictionary
        
    Returns:
        Dictionary containing data information
    """
    info = {
        "num_keys": len(data),
        "keys": list(data.keys()),
        "data_types": {},
        "shapes": {},
        "memory_usage": 0
    }
    
    for key, value in data.items():
        info["data_types"][key] = type(value).__name__
        
        if hasattr(value, "shape"):
            info["shapes"][key] = value.shape
        elif hasattr(value, "__len__"):
            info["shapes"][key] = len(value)
        
        # Estimate memory usage
        if hasattr(value, "nbytes"):
            info["memory_usage"] += value.nbytes
        elif hasattr(value, "__sizeof__"):
            info["memory_usage"] += value.__sizeof__()
    
    return info


def create_metadata(data: Dict[str, Any], source_path: Union[str, Path] = None) -> Dict[str, Any]:
    """
    Create metadata for data.
    
    Args:
        data: Data dictionary
        source_path: Optional source file path
        
    Returns:
        Metadata dictionary
    """
    metadata = {
        "data_info": get_data_info(data),
        "created_at": None,  # Can be filled with timestamp
        "source_path": str(source_path) if source_path else None,
        "format_version": "1.0",
    }
    
    if source_path:
        source_path = Path(source_path)
        if source_path.exists():
            metadata.update({
                "file_size": source_path.stat().st_size,
                "file_hash": calculate_file_hash(source_path),
            })
    
    return metadata


def save_metadata(metadata: Dict[str, Any], path: Union[str, Path]) -> None:
    """
    Save metadata to a JSON file.
    
    Args:
        metadata: Metadata dictionary
        path: Path to save metadata
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)


def load_metadata(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load metadata from a JSON file.
    
    Args:
        path: Path to metadata file
        
    Returns:
        Metadata dictionary
    """
    with open(path, 'r') as f:
        return json.load(f)
