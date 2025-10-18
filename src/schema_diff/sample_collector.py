"""Collect sample values from data records for display in comparisons."""

from typing import Any


def collect_field_samples(
    records: list[Any], max_samples: int = 5
) -> dict[str, list[Any]]:
    """Collect sample values for each field from a list of records.

    Parameters
    ----------
    records : list[Any]
        List of data records (typically dicts)
    max_samples : int
        Maximum number of sample values to collect per field

    Returns
    -------
    dict[str, list[Any]]
        Dictionary mapping field paths to lists of sample values
        Example: {"user.email": ["alice@example.com", "bob@example.com"], ...}
    """
    samples: dict[str, list[Any]] = {}

    def collect_from_value(value: Any, path: str = "") -> None:
        """Recursively collect samples from nested structures."""
        if value is None:
            # Store None as a sample
            if path and path not in samples:
                samples[path] = []
            if path and len(samples[path]) < max_samples:
                samples[path].append(None)
        elif isinstance(value, dict):
            for key, val in value.items():
                field_path = f"{path}.{key}" if path else key
                collect_from_value(val, field_path)
        elif isinstance(value, list):
            # For arrays, collect samples from first few elements
            for item in value[:max_samples]:
                collect_from_value(item, path)
        else:
            # Scalar value - collect it
            if path and path not in samples:
                samples[path] = []
            if path and len(samples[path]) < max_samples:
                # Store unique values only
                if value not in samples[path]:
                    samples[path].append(value)

    # Collect samples from each record
    for record in records:
        if len(samples) > 0:
            # Check if we have enough samples for all fields
            all_fields_have_enough = all(
                len(vals) >= max_samples for vals in samples.values()
            )
            if all_fields_have_enough:
                break
        collect_from_value(record)

    return samples


def format_sample_value(value: Any, max_length: int = 50) -> str:
    """Format a sample value for display.

    Parameters
    ----------
    value : Any
        The value to format
    max_length : int
        Maximum length of the formatted string

    Returns
    -------
    str
        Formatted string representation
    """
    if value is None:
        return "null"
    elif isinstance(value, str):
        # Quote strings and truncate if needed
        if len(value) > max_length:
            return f'"{value[:max_length-3]}..."'
        return f'"{value}"'
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, dict)):
        # For complex types, show type and length
        if isinstance(value, list):
            return f"[array of {len(value)} items]"
        else:
            return f"{{object with {len(value)} fields}}"
    else:
        # Fallback
        str_val = str(value)
        if len(str_val) > max_length:
            return f"{str_val[:max_length-3]}..."
        return str_val


def format_samples_for_field(
    field_path: str,
    left_samples: dict[str, list[Any]] | None,
    right_samples: dict[str, list[Any]] | None,
) -> str:
    """Format sample values for a field from both sides of a comparison.

    Parameters
    ----------
    field_path : str
        Dotted path to the field
    left_samples : dict[str, list[Any]] | None
        Sample values from left side
    right_samples : dict[str, list[Any]] | None
        Sample values from right side

    Returns
    -------
    str
        Formatted string showing samples from both sides
    """
    lines = []

    if left_samples and field_path in left_samples:
        left_vals = left_samples[field_path]
        formatted = [format_sample_value(v) for v in left_vals[:5]]
        lines.append(f"    Samples (left):  [{', '.join(formatted)}]")

    if right_samples and field_path in right_samples:
        right_vals = right_samples[field_path]
        formatted = [format_sample_value(v) for v in right_vals[:5]]
        lines.append(f"    Samples (right): [{', '.join(formatted)}]")

    return "\n".join(lines) if lines else ""


def format_samples_table(
    field_path: str,
    left_samples: dict[str, list[Any]] | None,
    right_samples: dict[str, list[Any]] | None,
    left_label: str = "Left",
    right_label: str = "Right",
) -> str:
    """Format sample values in a side-by-side table format.

    Parameters
    ----------
    field_path : str
        Dotted path to the field
    left_samples : dict[str, list[Any]] | None
        Sample values from left side
    right_samples : dict[str, list[Any]] | None
        Sample values from right side
    left_label : str
        Label for left side
    right_label : str
        Label for right side

    Returns
    -------
    str
        Formatted table showing samples side-by-side
    """
    # Get samples for this field
    left_vals = []
    right_vals = []

    if left_samples and field_path in left_samples:
        left_vals = left_samples[field_path][:5]

    if right_samples and field_path in right_samples:
        right_vals = right_samples[field_path][:5]

    # If no samples for either side, return empty
    if not left_vals and not right_vals:
        return ""

    # Format values
    left_formatted = [format_sample_value(v, max_length=40) for v in left_vals]
    right_formatted = [format_sample_value(v, max_length=40) for v in right_vals]

    # Pad to same length
    max_rows = max(len(left_formatted), len(right_formatted))
    left_formatted += [""] * (max_rows - len(left_formatted))
    right_formatted += [""] * (max_rows - len(right_formatted))

    # Calculate column widths
    left_width = max(len(left_label), max((len(v) for v in left_formatted), default=0))
    right_width = max(
        len(right_label), max((len(v) for v in right_formatted), default=0)
    )

    # Limit column widths
    left_width = min(left_width, 50)
    right_width = min(right_width, 50)

    # Build table
    lines = []
    lines.append(f"    {'─' * (left_width + right_width + 7)}")
    lines.append(f"    │ {left_label:<{left_width}} │ {right_label:<{right_width}} │")
    lines.append(f"    {'─' * (left_width + right_width + 7)}")

    for left_val, right_val in zip(left_formatted, right_formatted):
        # Truncate if needed
        left_display = left_val[:left_width] if len(left_val) > left_width else left_val
        right_display = (
            right_val[:right_width] if len(right_val) > right_width else right_val
        )
        lines.append(
            f"    │ {left_display:<{left_width}} │ {right_display:<{right_width}} │"
        )

    lines.append(f"    {'─' * (left_width + right_width + 7)}")

    return "\n".join(lines)
