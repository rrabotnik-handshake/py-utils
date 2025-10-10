"""
Type analysis utilities for schema comparison.

This module provides centralized logic for analyzing type representations,
separating base types from nullability, and determining the nature of type changes.
"""

from __future__ import annotations

from typing import Any


def extract_base_type_and_nullability(type_repr: str) -> tuple[str, bool]:
    """
    Extract base type and nullability from a type representation.

    Args:
        type_repr: Type representation like 'str', 'union(str|missing)', etc.

    Returns:
        Tuple of (base_type, is_nullable)

    Examples:
        >>> extract_base_type_and_nullability("str")
        ("str", False)
        >>> extract_base_type_and_nullability("union(str|missing)")
        ("str", True)
        >>> extract_base_type_and_nullability("union(str|int|missing)")
        ("union(str|int)", True)
        >>> extract_base_type_and_nullability("union(missing)")
        ("union(missing)", True)
    """
    if type_repr == "missing":
        return ("missing", True)

    if (
        isinstance(type_repr, str)
        and type_repr.startswith("union(")
        and type_repr.endswith(")")
    ):
        parts = type_repr[6:-1].split("|")
        if "missing" in parts:
            # Remove 'missing' and get the actual type
            non_missing_parts = [p for p in parts if p != "missing"]
            if len(non_missing_parts) == 0:
                # Only missing parts - return original type
                return (type_repr, True)
            elif len(non_missing_parts) == 1:
                return (non_missing_parts[0], True)
            else:
                # Multiple non-missing types - reconstruct the union without missing
                base_type = f"union({'|'.join(non_missing_parts)})"
                return (base_type, True)
        else:
            # Union without missing
            return (type_repr, False)

    # Simple type like 'str', 'int', etc.
    return (type_repr, False)


def analyze_type_change(old_type: str, new_type: str) -> dict[str, Any]:
    """
    Analyze a type change to determine if it's a type mismatch, nullability change, or both.

    Args:
        old_type: The old type representation
        new_type: The new type representation

    Returns:
        Dictionary with keys:
        - has_type_change: bool - whether the base types are different
        - has_nullability_change: bool - whether nullability differs
        - old_base_type: str - extracted base type from old_type
        - new_base_type: str - extracted base type from new_type
        - old_nullable: bool - whether old_type is nullable
        - new_nullable: bool - whether new_type is nullable

    Examples:
        >>> analyze_type_change("str", "union(str|missing)")
        {
            'has_type_change': False,
            'has_nullability_change': True,
            'old_base_type': 'str',
            'new_base_type': 'str',
            'old_nullable': False,
            'new_nullable': True
        }
        >>> analyze_type_change("union(str|int)", "union(str|float)")
        {
            'has_type_change': True,
            'has_nullability_change': False,
            'old_base_type': 'union(str|int)',
            'new_base_type': 'union(str|float)',
            'old_nullable': False,
            'new_nullable': False
        }
    """
    old_base, old_nullable = extract_base_type_and_nullability(old_type)
    new_base, new_nullable = extract_base_type_and_nullability(new_type)

    return {
        "has_type_change": old_base != new_base,
        "has_nullability_change": old_nullable != new_nullable,
        "old_base_type": old_base,
        "new_base_type": new_base,
        "old_nullable": old_nullable,
        "new_nullable": new_nullable,
    }


def is_presence_issue(old_type: str, new_type: str) -> bool:
    """
    Determine if a type change is primarily about presence/optionality rather than type.

    This is a convenience function that wraps analyze_type_change for backward compatibility.

    Args:
        old_type: The old type representation
        new_type: The new type representation

    Returns:
        True if this is primarily a presence issue (nullability change without type change)
        False if this is primarily a type issue (type change, with or without nullability change)
    """
    analysis = analyze_type_change(old_type, new_type)
    return analysis["has_nullability_change"] and not analysis["has_type_change"]
