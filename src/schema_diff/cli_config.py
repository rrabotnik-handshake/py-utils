#!/usr/bin/env python3
"""
Configuration management for schema-diff.

Supports:
- YAML configuration files
- Environment variable overrides
- Default values
- Validation
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class SchemadiffConfig:
    """Configuration for schema-diff with defaults."""
    
    # BigQuery settings
    default_project: Optional[str] = None
    default_dataset: Optional[str] = None
    
    # Output settings
    color_mode: str = "auto"  # auto, always, never
    output_format: str = "pretty"  # pretty, compact
    include_constraints: bool = True
    
    # Comparison settings
    default_samples: int = 3
    infer_datetimes: bool = False
    show_presence: bool = True
    
    # File patterns
    exclude_patterns: list[str] = field(default_factory=lambda: [
        ".*_temp",
        ".*_staging", 
        "test_.*"
    ])
    
    # Paths
    output_dir: Optional[str] = None
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "SchemadiffConfig":
        """Load configuration from file and environment."""
        config = cls()
        
        # Load from config file
        if config_path or cls._find_config_file():
            config_file = config_path or cls._find_config_file()
            if config_file and Path(config_file).exists():
                config._load_from_file(config_file)
        
        # Override with environment variables
        config._load_from_env()
        
        return config
    
    @staticmethod
    def _find_config_file() -> Optional[str]:
        """Find config file in standard locations."""
        candidates = [
            "schema-diff.yml",
            "schema-diff.yaml", 
            ".schema-diff.yml",
            ".schema-diff.yaml",
            os.path.expanduser("~/.schema-diff.yml"),
            os.path.expanduser("~/.schema-diff.yaml"),
            os.path.expanduser("~/.config/schema-diff/config.yml"),
            os.path.expanduser("~/.config/schema-diff/config.yaml"),
        ]
        
        for candidate in candidates:
            if Path(candidate).exists():
                return candidate
        return None
    
    def _load_from_file(self, config_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            import yaml
        except ImportError:
            print("Warning: PyYAML not installed, skipping config file", file=sys.stderr)
            return
        
        try:
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f) or {}
            
            # Update fields that exist in the dataclass
            for key, value in data.items():
                if hasattr(self, key):
                    setattr(self, key, value)
                    
        except Exception as e:
            print(f"Warning: Error loading config from {config_path}: {e}", file=sys.stderr)
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        env_mapping = {
            "SCHEMA_DIFF_PROJECT": "default_project",
            "SCHEMA_DIFF_DATASET": "default_dataset", 
            "SCHEMA_DIFF_COLOR": "color_mode",
            "SCHEMA_DIFF_FORMAT": "output_format",
            "SCHEMA_DIFF_CONSTRAINTS": "include_constraints",
            "SCHEMA_DIFF_SAMPLES": "default_samples",
            "SCHEMA_DIFF_INFER_DATES": "infer_datetimes",
            "SCHEMA_DIFF_SHOW_PRESENCE": "show_presence",
            "SCHEMA_DIFF_OUTPUT_DIR": "output_dir",
        }
        
        for env_var, attr_name in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                # Type conversion
                if attr_name in ("include_constraints", "infer_datetimes", "show_presence"):
                    value = value.lower() in ("true", "1", "yes", "on")
                elif attr_name == "default_samples":
                    try:
                        value = int(value)
                    except ValueError:
                        continue
                
                setattr(self, attr_name, value)
    
    def save(self, config_path: str) -> None:
        """Save current configuration to file."""
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML required to save configuration")
        
        # Convert to dict, excluding None values
        data = {}
        for key, value in self.__dict__.items():
            if value is not None:
                data[key] = value
        
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=True)


def add_config_subcommands(subparsers) -> None:
    """Add configuration management subcommands."""
    
    # schema-diff config show
    config_show = subparsers.add_parser(
        "config-show",
        help="Show current configuration",
        description="Display current configuration values from files and environment."
    )
    config_show.add_argument(
        "--config", metavar="PATH",
        help="Path to config file (default: auto-discover)"
    )
    config_show.set_defaults(func=cmd_config_show)
    
    # schema-diff config init
    config_init = subparsers.add_parser(
        "config-init",
        help="Initialize configuration file", 
        description="Create a new configuration file with default values."
    )
    config_init.add_argument(
        "config_path", nargs="?", default="schema-diff.yml",
        help="Path for new config file (default: schema-diff.yml)"
    )
    config_init.add_argument(
        "--project", help="Default BigQuery project"
    )
    config_init.add_argument(
        "--dataset", help="Default BigQuery dataset"
    )
    config_init.add_argument(
        "--force", action="store_true",
        help="Overwrite existing config file"
    )
    config_init.set_defaults(func=cmd_config_init)


def cmd_config_show(args) -> None:
    """Handle 'schema-diff config-show' command."""
    try:
        config = SchemadiffConfig.load(args.config)
        
        print("Current schema-diff configuration:")
        print("=" * 40)
        
        # Group settings by category
        categories = {
            "BigQuery": ["default_project", "default_dataset"],
            "Output": ["color_mode", "output_format", "include_constraints", "output_dir"],
            "Comparison": ["default_samples", "infer_datetimes", "show_presence"],
            "Filtering": ["exclude_patterns"],
        }
        
        for category, keys in categories.items():
            print(f"\n{category}:")
            for key in keys:
                value = getattr(config, key, None)
                if isinstance(value, list):
                    value = ", ".join(value) if value else "(none)"
                elif value is None:
                    value = "(not set)"
                print(f"  {key}: {value}")
        
        # Show config file location
        config_file = args.config or SchemadiffConfig._find_config_file()
        if config_file:
            print(f"\nConfig file: {config_file}")
        else:
            print("\nNo config file found (using defaults)")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_config_init(args) -> None:
    """Handle 'schema-diff config-init' command."""
    try:
        config_path = Path(args.config_path)
        
        if config_path.exists() and not args.force:
            print(f"Error: Config file {config_path} already exists. Use --force to overwrite.", file=sys.stderr)
            sys.exit(1)
        
        # Create config with any provided values
        config = SchemadiffConfig()
        if args.project:
            config.default_project = args.project
        if args.dataset:
            config.default_dataset = args.dataset
        
        config.save(str(config_path))
        print(f"Created config file: {config_path}")
        
        # Show what was created
        print(f"\nGenerated configuration:")
        print("-" * 30)
        with open(config_path, 'r') as f:
            print(f.read())
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
