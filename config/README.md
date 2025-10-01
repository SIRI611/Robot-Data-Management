# Robot Data Management (RDM) Configuration

This directory contains configuration files and examples for RDM.

## Configuration Files

- `default_config.json`: Default configuration settings
- `example_config.json`: Example configuration with custom settings

## Usage

```python
from rdm.config import config

# Load custom configuration
config.load_config("config/example_config.json")

# Access configuration values
compression = config.get("formats.hdf5.compression")
validation_strict = config.get("validation.strict")

# Set configuration values
config.set("formats.hdf5.compression", "lzf")
config.set("validation.strict", False)
```

## Configuration Schema

The configuration follows a hierarchical structure:

```json
{
  "formats": {
    "hdf5": {
      "compression": "gzip",
      "compression_opts": 9,
      "chunk_size": null
    },
    "zarr": {
      "compression": "blosc",
      "chunk_size": "auto"
    },
    "json": {
      "indent": 2,
      "ensure_ascii": false
    },
    "pickle": {
      "protocol": "highest"
    },
    "numpy": {
      "allow_pickle": true
    }
  },
  "validation": {
    "strict": true,
    "check_schema": true
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  },
  "conversion": {
    "parallel": true,
    "num_workers": null,
    "batch_size": 1
  }
}
```
