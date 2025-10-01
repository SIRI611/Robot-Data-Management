"""
Example usage of Robot Data Management (RDM).

This module demonstrates how to use RDM for various data management tasks.
"""

import numpy as np
from pathlib import Path
from rdm import DataManager, FormatType


def create_sample_data():
    """Create sample robot data for demonstration."""
    return {
        "observations": np.random.randn(100, 10),
        "actions": np.random.randn(100, 7),
        "rewards": np.random.randn(100),
        "metadata": {
            "episode_length": 100,
            "robot_type": "manipulator",
            "task": "pick_and_place",
        }
    }


def basic_usage_example():
    """Demonstrate basic RDM usage."""
    print("=== Basic Usage Example ===")
    
    # Create sample data
    data = create_sample_data()
    
    # Initialize data manager
    dm = DataManager()
    
    # Save data in different formats
    dm.save(data, "sample_data.h5", FormatType.HDF5)
    dm.save(data, "sample_data.zarr", FormatType.ZARR)
    dm.save(data, "sample_data.json", FormatType.JSON)
    
    print("✓ Saved data in multiple formats")
    
    # Load data from different formats
    hdf5_data = dm.load("sample_data.h5", FormatType.HDF5)
    zarr_data = dm.load("sample_data.zarr", FormatType.ZARR)
    json_data = dm.load("sample_data.json", FormatType.JSON)
    
    print("✓ Loaded data from multiple formats")
    
    # Validate data
    is_valid_hdf5 = dm.validate("sample_data.h5", FormatType.HDF5)
    is_valid_zarr = dm.validate("sample_data.zarr", FormatType.ZARR)
    is_valid_json = dm.validate("sample_data.json", FormatType.JSON)
    
    print(f"✓ Validation results: HDF5={is_valid_hdf5}, Zarr={is_valid_zarr}, JSON={is_valid_json}")


def conversion_example():
    """Demonstrate data conversion between formats."""
    print("\n=== Conversion Example ===")
    
    dm = DataManager()
    
    # Convert between formats
    dm.convert("sample_data.h5", "converted_data.zarr", FormatType.HDF5, FormatType.ZARR)
    dm.convert("sample_data.zarr", "converted_data.json", FormatType.ZARR, FormatType.JSON)
    dm.convert("sample_data.json", "converted_data.pkl", FormatType.JSON, FormatType.PICKLE)
    
    print("✓ Converted data between multiple formats")


def batch_processing_example():
    """Demonstrate batch processing."""
    print("\n=== Batch Processing Example ===")
    
    # Create multiple sample files
    dm = DataManager()
    data = create_sample_data()
    
    # Create input directory with multiple files
    input_dir = Path("input_data")
    input_dir.mkdir(exist_ok=True)
    
    for i in range(3):
        dm.save(data, input_dir / f"episode_{i}.h5", FormatType.HDF5)
    
    print("✓ Created multiple input files")
    
    # Batch convert
    dm.batch_convert(
        "input_data/",
        "output_data/",
        FormatType.HDF5,
        FormatType.RLDS
    )
    
    print("✓ Batch converted files")


def metadata_example():
    """Demonstrate metadata handling."""
    print("\n=== Metadata Example ===")
    
    from rdm.utils import create_metadata, save_metadata, load_metadata
    
    data = create_sample_data()
    
    # Create metadata
    metadata = create_metadata(data, "sample_data.h5")
    print(f"✓ Created metadata: {list(metadata.keys())}")
    
    # Save metadata
    save_metadata(metadata, "sample_data_metadata.json")
    print("✓ Saved metadata to file")
    
    # Load metadata
    loaded_metadata = load_metadata("sample_data_metadata.json")
    print(f"✓ Loaded metadata: {loaded_metadata['data_info']['num_keys']} keys")


def configuration_example():
    """Demonstrate configuration management."""
    print("\n=== Configuration Example ===")
    
    from rdm.config import config
    
    # Get configuration values
    hdf5_compression = config.get("formats.hdf5.compression")
    validation_strict = config.get("validation.strict")
    
    print(f"✓ HDF5 compression: {hdf5_compression}")
    print(f"✓ Validation strict: {validation_strict}")
    
    # Set configuration values
    config.set("formats.hdf5.compression", "lzf")
    config.set("validation.strict", False)
    
    print("✓ Updated configuration")


def cleanup_example_files():
    """Clean up example files."""
    import shutil
    
    files_to_remove = [
        "sample_data.h5",
        "sample_data.zarr", 
        "sample_data.json",
        "converted_data.zarr",
        "converted_data.json",
        "converted_data.pkl",
        "sample_data_metadata.json",
    ]
    
    dirs_to_remove = ["input_data", "output_data"]
    
    for file in files_to_remove:
        if Path(file).exists():
            Path(file).unlink()
    
    for dir in dirs_to_remove:
        if Path(dir).exists():
            shutil.rmtree(dir)
    
    print("\n✓ Cleaned up example files")


if __name__ == "__main__":
    try:
        basic_usage_example()
        conversion_example()
        batch_processing_example()
        metadata_example()
        configuration_example()
        
        print("\n=== All Examples Completed Successfully! ===")
        
    except Exception as e:
        print(f"✗ Example failed: {e}")
        raise
    
    finally:
        cleanup_example_files()
