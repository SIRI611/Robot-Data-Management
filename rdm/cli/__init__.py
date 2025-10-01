"""
Command-line interface for Robot Data Management.

This module provides CLI commands for data conversion, validation, and management.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from ..core import DataManager, FormatType
from ..config import config

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    """
    Setup logging configuration.
    
    Args:
        level: Logging level
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=config.get("logging.format"),
    )


def convert_command(args) -> None:
    """Handle convert command."""
    dm = DataManager()
    
    try:
        dm.convert(
            source_path=args.source,
            target_path=args.target,
            source_format=FormatType(args.source_format) if args.source_format else FormatType.AUTO,
            target_format=FormatType(args.target_format) if args.target_format else FormatType.AUTO,
        )
        print(f"Successfully converted {args.source} to {args.target}")
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)


def batch_convert_command(args) -> None:
    """Handle batch convert command."""
    dm = DataManager()
    
    try:
        dm.batch_convert(
            source_dir=args.source_dir,
            target_dir=args.target_dir,
            source_format=FormatType(args.source_format) if args.source_format else FormatType.AUTO,
            target_format=FormatType(args.target_format) if args.target_format else FormatType.AUTO,
        )
        print(f"Successfully batch converted {args.source_dir} to {args.target_dir}")
    except Exception as e:
        logger.error(f"Batch conversion failed: {e}")
        sys.exit(1)


def validate_command(args) -> None:
    """Handle validate command."""
    dm = DataManager()
    
    try:
        is_valid = dm.validate(
            path=args.path,
            format_type=FormatType(args.format) if args.format else FormatType.AUTO,
        )
        
        if is_valid:
            print(f"✓ {args.path} is valid")
        else:
            print(f"✗ {args.path} is invalid")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


def info_command(args) -> None:
    """Handle info command."""
    dm = DataManager()
    
    try:
        format_type = FormatType(args.format) if args.format else FormatType.AUTO
        loader_class = dm._loaders.get(format_type)
        
        if not loader_class:
            print(f"No loader available for format: {format_type}")
            sys.exit(1)
        
        loader = loader_class(args.path)
        metadata = loader.get_metadata()
        
        print(f"File: {args.path}")
        print(f"Format: {format_type.value}")
        print(f"Size: {metadata.get('file_size', 'Unknown')} bytes")
        
        if 'groups' in metadata:
            print(f"Groups: {metadata['groups']}")
        
    except Exception as e:
        logger.error(f"Info command failed: {e}")
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Robot Data Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Convert command
    convert_parser = subparsers.add_parser("convert", help="Convert data between formats")
    convert_parser.add_argument("source", help="Source file path")
    convert_parser.add_argument("target", help="Target file path")
    convert_parser.add_argument(
        "--source-format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Source format (auto-detect if not specified)",
    )
    convert_parser.add_argument(
        "--target-format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Target format (auto-detect if not specified)",
    )
    
    # Batch convert command
    batch_parser = subparsers.add_parser("batch-convert", help="Batch convert multiple files")
    batch_parser.add_argument("source_dir", help="Source directory")
    batch_parser.add_argument("target_dir", help="Target directory")
    batch_parser.add_argument(
        "--source-format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Source format (auto-detect if not specified)",
    )
    batch_parser.add_argument(
        "--target-format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Target format (auto-detect if not specified)",
    )
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate data file")
    validate_parser.add_argument("path", help="File path to validate")
    validate_parser.add_argument(
        "--format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Format (auto-detect if not specified)",
    )
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Get file information")
    info_parser.add_argument("path", help="File path")
    info_parser.add_argument(
        "--format",
        choices=[f.value for f in FormatType if f != FormatType.AUTO],
        help="Format (auto-detect if not specified)",
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Load configuration if specified
    if args.config:
        config.load_config(args.config)
    
    # Execute command
    if args.command == "convert":
        convert_command(args)
    elif args.command == "batch-convert":
        batch_convert_command(args)
    elif args.command == "validate":
        validate_command(args)
    elif args.command == "info":
        info_command(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
