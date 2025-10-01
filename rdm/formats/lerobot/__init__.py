"""
LeRobot data format implementation for Robot Data Management.

This module provides LeRobot-specific data loading, saving, and conversion functionality.
"""

import logging
from typing import Any, Dict, Optional, Union
from pathlib import Path

import json
import numpy as np

from ..core import BaseDataLoader, BaseDataConverter, FormatType, DataValidationError

logger = logging.getLogger(__name__)


class LeRobotLoader(BaseDataLoader):
    """LeRobot data loader implementation."""
    
    def __init__(self, path: Union[str, Path], **kwargs):
        """
        Initialize LeRobot loader.
        
        Args:
            path: Path to LeRobot dataset
            **kwargs: Additional parameters
        """
        super().__init__(path, **kwargs)
    
    def load(self) -> Dict[str, Any]:
        """
        Load data from LeRobot dataset.
        
        Returns:
            Dictionary containing loaded data
        """
        try:
            if self.path.is_file():
                # Single file
                data = self._load_single_file()
            else:
                # Directory with multiple files
                data = self._load_directory()
            
            logger.info(f"Successfully loaded LeRobot data from {self.path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load LeRobot data from {self.path}: {e}")
            raise DataValidationError(f"LeRobot loading failed: {e}")
    
    def save(self, data: Dict[str, Any], path: Optional[Union[str, Path]] = None) -> None:
        """
        Save data to LeRobot format.
        
        Args:
            data: Data dictionary to save
            path: Optional path to save to
        """
        save_path = Path(path) if path else self.path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Convert data to LeRobot format
            lerobot_data = self._convert_to_lerobot_format(data)
            
            # Save as JSON
            with open(save_path, 'w') as f:
                json.dump(lerobot_data, f, indent=2, default=self._json_serializer)
            
            logger.info(f"Successfully saved LeRobot data to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save LeRobot data to {save_path}: {e}")
            raise DataValidationError(f"LeRobot saving failed: {e}")
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate LeRobot data structure.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            True if data is valid
        """
        try:
            # Check if data has required LeRobot structure
            required_keys = ["episodes"]
            if not all(key in data for key in required_keys):
                return False
            
            # Validate episodes structure
            episodes = data["episodes"]
            if not isinstance(episodes, list):
                return False
            
            # Validate each episode
            for episode in episodes:
                if not self._validate_episode(episode):
                    return False
            
            return True
        except Exception as e:
            logger.warning(f"LeRobot validation failed: {e}")
            return False
    
    def _load_single_file(self) -> Dict[str, Any]:
        """
        Load data from a single file.
        
        Returns:
            Loaded data dictionary
        """
        if self.path.suffix == '.json':
            with open(self.path, 'r') as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported file format: {self.path.suffix}")
    
    def _load_directory(self) -> Dict[str, Any]:
        """
        Load data from a directory structure.
        
        Returns:
            Loaded data dictionary
        """
        episodes = []
        
        # Look for episode files
        for episode_file in self.path.glob("episode_*.json"):
            with open(episode_file, 'r') as f:
                episode_data = json.load(f)
                episodes.append(episode_data)
        
        return {
            "episodes": episodes,
            "metadata": {
                "num_episodes": len(episodes),
                "format": "lerobot"
            }
        }
    
    def _validate_episode(self, episode: Dict[str, Any]) -> bool:
        """
        Validate a single episode.
        
        Args:
            episode: Episode data dictionary
            
        Returns:
            True if episode is valid
        """
        required_keys = ["episode_id", "steps"]
        if not all(key in episode for key in required_keys):
            return False
        
        # Validate steps
        steps = episode["steps"]
        if not isinstance(steps, list):
            return False
        
        return True
    
    def _convert_to_lerobot_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert data dictionary to LeRobot format.
        
        Args:
            data: Data dictionary to convert
            
        Returns:
            Data in LeRobot format
        """
        # Convert data to LeRobot episode format
        if "episodes" in data:
            return data
        else:
            # Convert single episode format
            return {
                "episodes": [data],
                "metadata": {
                    "num_episodes": 1,
                    "format": "lerobot"
                }
            }
    
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
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get LeRobot dataset metadata.
        
        Returns:
            Dictionary containing metadata
        """
        metadata = super().get_metadata()
        
        try:
            if self.path.is_file():
                metadata.update({
                    "file_type": "single_file",
                    "file_size": self.path.stat().st_size,
                })
            else:
                episode_files = list(self.path.glob("episode_*.json"))
                metadata.update({
                    "file_type": "directory",
                    "num_episode_files": len(episode_files),
                })
        except Exception as e:
            logger.warning(f"Failed to get LeRobot metadata: {e}")
        
        return metadata


class LeRobotConverter(BaseDataConverter):
    """LeRobot data converter implementation."""
    
    def convert_from(self, source_loader: BaseDataLoader) -> Dict[str, Any]:
        """
        Convert data from source format to LeRobot-compatible format.
        
        Args:
            source_loader: Source data loader
            
        Returns:
            Converted data dictionary
        """
        data = source_loader.load()
        
        # LeRobot-specific conversion logic can be added here
        # For now, return data as-is
        return data
    
    def convert_to(self, data: Dict[str, Any], target_loader: BaseDataLoader) -> None:
        """
        Convert LeRobot data to target format.
        
        Args:
            data: Data dictionary to convert
            target_loader: Target data loader
        """
        # Save data using target loader
        target_loader.save(data)
