# Robot Data Management (RDM)

A comprehensive Python library for managing robot data across multiple formats including HDF5, Zarr, RLDS, LeRobot, and more.

## Features

- **Multi-format Support**: HDF5, Zarr, RLDS, LeRobot, JSON, Pickle, NumPy
- **Unified API**: Consistent interface across all data formats
- **Data Conversion**: Convert between different formats seamlessly
- **Validation**: Built-in data validation and integrity checks
- **Metadata Management**: Comprehensive metadata handling and schema validation
- **Performance**: Optimized for large-scale robot datasets
- **Extensible**: Easy to add new data formats

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from rdm import DataManager, FormatType

# Initialize data manager
dm = DataManager()

# Load data from any supported format
data = dm.load("path/to/data.h5", format_type=FormatType.HDF5)

# Convert between formats
dm.convert("input.h5", "output.zarr", FormatType.HDF5, FormatType.ZARR)

# Batch process multiple files
dm.batch_convert("input_dir/", "output_dir/", FormatType.HDF5, FormatType.RLDS)
```

## Supported Formats

- **HDF5**: High-performance hierarchical data format
- **Zarr**: Cloud-native array storage
- **RLDS**: Reinforcement Learning Dataset format
- **LeRobot**: Hugging Face LeRobot format
- **JSON**: Human-readable structured data
- **Pickle**: Python object serialization
- **NumPy**: Native Python array format

## Command Line Interface

```bash
# Convert between formats
rdm convert input.h5 output.zarr --source-format hdf5 --target-format zarr

# Batch convert multiple files
rdm batch-convert input_dir/ output_dir/ --source-format hdf5 --target-format rlds

# Validate data files
rdm validate data.h5 --format hdf5

# Get file information
rdm info data.h5 --format hdf5
```

## Examples

### Basic Usage

```python
from rdm import DataManager, FormatType

# Initialize data manager
dm = DataManager()

# Load data
data = dm.load("robot_data.h5", FormatType.HDF5)

# Convert to different format
dm.convert("robot_data.h5", "robot_data.zarr", FormatType.HDF5, FormatType.ZARR)

# Validate data
is_valid = dm.validate("robot_data.h5", FormatType.HDF5)
```

### Custom Configuration

```python
from rdm import DataManager, FormatType
from rdm.config import config

# Set custom configuration
config.set("formats.hdf5.compression", "lzf")
config.set("formats.hdf5.compression_opts", 5)

# Initialize with custom config
dm = DataManager(config.config)
```

### Batch Processing

```python
from rdm import DataManager, FormatType

dm = DataManager()

# Convert entire directory
dm.batch_convert(
    "input_data/",
    "output_data/",
    FormatType.HDF5,
    FormatType.RLDS
)
```

## Architecture

The library is built with a modular architecture:

- **Core**: Base classes and interfaces
- **Formats**: Format-specific implementations
- **Utils**: Common utility functions
- **Config**: Configuration management
- **CLI**: Command-line interface
