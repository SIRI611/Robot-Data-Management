"""
Test suite for Robot Data Management (RDM).

This module contains unit tests for all RDM components.
"""

import pytest
import numpy as np
import tempfile
import shutil
from pathlib import Path

from rdm import DataManager, FormatType, DataValidationError
from rdm.core import BaseDataLoader, BaseDataConverter
from rdm.formats import (
    HDF5Loader, ZarrLoader, JSONLoader, PickleLoader, NumPyLoader
)
from rdm.utils import flatten_dict, unflatten_dict, calculate_file_hash
from rdm.config import Config


class TestDataManager:
    """Test cases for DataManager."""
    
    def setup_method(self):
        """Setup test data."""
        self.dm = DataManager()
        self.sample_data = {
            "observations": np.random.randn(10, 5),
            "actions": np.random.randn(10, 3),
            "metadata": {
                "episode_id": 1,
                "task": "test_task",
            }
        }
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test data."""
        shutil.rmtree(self.temp_dir)
    
    def test_load_hdf5(self):
        """Test loading HDF5 data."""
        # Save test data
        test_file = Path(self.temp_dir) / "test.h5"
        self.dm.save(self.sample_data, test_file, FormatType.HDF5)
        
        # Load and verify
        loaded_data = self.dm.load(test_file, FormatType.HDF5)
        assert "observations" in loaded_data
        assert "actions" in loaded_data
        assert "metadata" in loaded_data
        assert loaded_data["metadata"]["episode_id"] == 1
    
    def test_load_json(self):
        """Test loading JSON data."""
        # Save test data
        test_file = Path(self.temp_dir) / "test.json"
        self.dm.save(self.sample_data, test_file, FormatType.JSON)
        
        # Load and verify
        loaded_data = self.dm.load(test_file, FormatType.JSON)
        assert "observations" in loaded_data
        assert "actions" in loaded_data
        assert "metadata" in loaded_data
    
    def test_convert_hdf5_to_json(self):
        """Test converting HDF5 to JSON."""
        # Save test data
        hdf5_file = Path(self.temp_dir) / "test.h5"
        json_file = Path(self.temp_dir) / "test.json"
        
        self.dm.save(self.sample_data, hdf5_file, FormatType.HDF5)
        
        # Convert
        self.dm.convert(hdf5_file, json_file, FormatType.HDF5, FormatType.JSON)
        
        # Verify conversion
        assert json_file.exists()
        loaded_data = self.dm.load(json_file, FormatType.JSON)
        assert "observations" in loaded_data
        assert "metadata" in loaded_data
    
    def test_validate_data(self):
        """Test data validation."""
        # Save test data
        test_file = Path(self.temp_dir) / "test.h5"
        self.dm.save(self.sample_data, test_file, FormatType.HDF5)
        
        # Validate
        is_valid = self.dm.validate(test_file, FormatType.HDF5)
        assert is_valid
    
    def test_auto_detect_format(self):
        """Test automatic format detection."""
        # Save test data
        hdf5_file = Path(self.temp_dir) / "test.h5"
        json_file = Path(self.temp_dir) / "test.json"
        
        self.dm.save(self.sample_data, hdf5_file, FormatType.HDF5)
        self.dm.save(self.sample_data, json_file, FormatType.JSON)
        
        # Test auto-detection
        hdf5_data = self.dm.load(hdf5_file, FormatType.AUTO)
        json_data = self.dm.load(json_file, FormatType.AUTO)
        
        assert "observations" in hdf5_data
        assert "observations" in json_data


class TestHDF5Loader:
    """Test cases for HDF5Loader."""
    
    def setup_method(self):
        """Setup test data."""
        self.loader = HDF5Loader("dummy_path")
        self.sample_data = {
            "array_data": np.random.randn(10, 5),
            "scalar_data": 42,
            "nested_data": {
                "inner_array": np.random.randn(5),
                "inner_scalar": "test",
            }
        }
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test data."""
        shutil.rmtree(self.temp_dir)
    
    def test_save_and_load(self):
        """Test saving and loading HDF5 data."""
        test_file = Path(self.temp_dir) / "test.h5"
        
        # Save data
        self.loader.save(self.sample_data, test_file)
        assert test_file.exists()
        
        # Load data
        loaded_data = self.loader.load()
        assert "array_data" in loaded_data
        assert "scalar_data" in loaded_data
        assert "nested_data" in loaded_data
        assert loaded_data["scalar_data"] == 42
    
    def test_validate_data(self):
        """Test data validation."""
        test_file = Path(self.temp_dir) / "test.h5"
        
        # Save data
        self.loader.save(self.sample_data, test_file)
        
        # Validate
        is_valid = self.loader.validate(self.sample_data)
        assert is_valid


class TestJSONLoader:
    """Test cases for JSONLoader."""
    
    def setup_method(self):
        """Setup test data."""
        self.loader = JSONLoader("dummy_path")
        self.sample_data = {
            "array_data": np.random.randn(10, 5).tolist(),
            "scalar_data": 42,
            "nested_data": {
                "inner_array": np.random.randn(5).tolist(),
                "inner_scalar": "test",
            }
        }
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test data."""
        shutil.rmtree(self.temp_dir)
    
    def test_save_and_load(self):
        """Test saving and loading JSON data."""
        test_file = Path(self.temp_dir) / "test.json"
        
        # Save data
        self.loader.save(self.sample_data, test_file)
        assert test_file.exists()
        
        # Load data
        loaded_data = self.loader.load()
        assert "array_data" in loaded_data
        assert "scalar_data" in loaded_data
        assert "nested_data" in loaded_data
        assert loaded_data["scalar_data"] == 42


class TestUtils:
    """Test cases for utility functions."""
    
    def test_flatten_dict(self):
        """Test dictionary flattening."""
        nested_dict = {
            "a": 1,
            "b": {
                "c": 2,
                "d": {
                    "e": 3
                }
            }
        }
        
        flattened = flatten_dict(nested_dict)
        expected = {"a": 1, "b.c": 2, "b.d.e": 3}
        assert flattened == expected
    
    def test_unflatten_dict(self):
        """Test dictionary unflattening."""
        flattened_dict = {"a": 1, "b.c": 2, "b.d.e": 3}
        
        unflattened = unflatten_dict(flattened_dict)
        expected = {
            "a": 1,
            "b": {
                "c": 2,
                "d": {
                    "e": 3
                }
            }
        }
        assert unflattened == expected
    
    def test_calculate_file_hash(self):
        """Test file hash calculation."""
        test_file = Path(tempfile.mktemp())
        
        # Create test file
        with open(test_file, "w") as f:
            f.write("test content")
        
        try:
            hash_value = calculate_file_hash(test_file)
            assert isinstance(hash_value, str)
            assert len(hash_value) == 64  # SHA256 hash length
        finally:
            test_file.unlink()


class TestConfig:
    """Test cases for configuration management."""
    
    def setup_method(self):
        """Setup test configuration."""
        self.config = Config()
    
    def test_get_set_config(self):
        """Test getting and setting configuration values."""
        # Test getting default value
        compression = self.config.get("formats.hdf5.compression")
        assert compression == "gzip"
        
        # Test setting value
        self.config.set("formats.hdf5.compression", "lzf")
        compression = self.config.get("formats.hdf5.compression")
        assert compression == "lzf"
    
    def test_get_nonexistent_key(self):
        """Test getting non-existent configuration key."""
        value = self.config.get("nonexistent.key", "default")
        assert value == "default"


if __name__ == "__main__":
    pytest.main([__file__])
