"""
Shared utilities for consistent file output across schema-diff tools.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def ensure_output_dir(subdir: Optional[str] = None) -> Path:
    """
    Ensure the output directory exists and return the path.

    Args:
        subdir: Optional subdirectory within ./output

    Returns:
        Path to the output directory
    """
    if subdir:
        output_dir = Path("./output") / subdir
    else:
        output_dir = Path("./output")

    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_output_file(
    content: str, filename: str, subdir: Optional[str] = None
) -> Path:
    """
    Write content to a file in the output directory.

    Args:
        content: Content to write
        filename: Name of the file
        subdir: Optional subdirectory within ./output

    Returns:
        Path to the written file
    """
    output_dir = ensure_output_dir(subdir)
    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    return output_path


def write_json_output(
    data: Dict[str, Any], filename: str, subdir: Optional[str] = None
) -> Path:
    """
    Write JSON data to a file in the output directory.

    Args:
        data: Data to write as JSON
        filename: Name of the file (should end with .json)
        subdir: Optional subdirectory within ./output

    Returns:
        Path to the written file
    """
    output_dir = ensure_output_dir(subdir)
    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return output_path


def generate_timestamp_filename(base_name: str, extension: str = ".txt") -> str:
    """
    Generate a filename with timestamp to avoid conflicts.

    Args:
        base_name: Base name for the file
        extension: File extension (with dot)

    Returns:
        Filename with timestamp
    """
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}{extension}"


def print_output_success(output_path: Path, description: str = "File") -> None:
    """
    Print a standardized success message for file output.

    Args:
        output_path: Path to the written file
        description: Description of what was written
    """
    print(f"✅ {description} written to: {output_path}")
    print(f"📁 Output directory: {output_path.parent.absolute()}")
