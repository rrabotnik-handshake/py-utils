#!/usr/bin/env python3
"""Decorators for schema-diff operations.

Provides caching, retry, and other cross-cutting concerns as decorators.
"""
from __future__ import annotations

import functools
import hashlib
import json
import os
import pickle  # nosec B403: pickle is used safely for internal caching only
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class CacheManager:
    """Manages caching for expensive operations."""

    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = Path(cache_dir or os.path.expanduser("~/.cache/schema-diff"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache: Dict[str, Any] = {}

    def _get_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate a cache key from function name and arguments."""
        # Create a stable hash from function name and arguments
        key_data = {"func": func_name, "args": args, "kwargs": sorted(kwargs.items())}
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(
            key_str.encode(), usedforsecurity=False
        ).hexdigest()  # trunk-ignore(bandit/B324)

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the file path for a cache key."""
        return self.cache_dir / f"{cache_key}.cache"

    def get(self, func_name: str, args: tuple, kwargs: dict) -> tuple[bool, Any]:
        """Get cached result if available."""
        cache_key = self._get_cache_key(func_name, args, kwargs)

        # Check memory cache first
        if cache_key in self._memory_cache:
            return True, self._memory_cache[cache_key]

        # Check disk cache
        cache_path = self._get_cache_path(cache_key)
        if cache_path.exists():
            try:
                with open(cache_path, "rb") as f:
                    result = pickle.load(
                        f
                    )  # nosec B301: loading trusted internal cache files only
                # Store in memory cache for faster access
                self._memory_cache[cache_key] = result
                return True, result
            except Exception:
                # Cache file corrupted, remove it
                cache_path.unlink(missing_ok=True)

        return False, None

    def set(self, func_name: str, args: tuple, kwargs: dict, result: Any) -> None:
        """Cache a result."""
        cache_key = self._get_cache_key(func_name, args, kwargs)

        # Store in memory cache
        self._memory_cache[cache_key] = result

        # Store in disk cache
        cache_path = self._get_cache_path(cache_key)
        try:
            with open(cache_path, "wb") as f:
                pickle.dump(result, f)
        except Exception:
            # If we can't write to disk cache, that's okay - continue without caching
            return

    def clear(self) -> None:
        """Clear all caches."""
        self._memory_cache.clear()
        for cache_file in self.cache_dir.glob("*.cache"):
            cache_file.unlink(missing_ok=True)


# Global cache manager instance
_cache_manager = CacheManager()


def cache_results(
    ttl_seconds: Optional[int] = None,
    memory_only: bool = False,
    cache_key_func: Optional[Callable] = None,
) -> Callable[[F], F]:
    """Decorator to cache function results.

    Args:
        ttl_seconds: Time-to-live for cache entries (None = no expiration) [TODO: Not implemented yet]
        memory_only: Only use memory cache, not disk cache [TODO: Not implemented yet]
        cache_key_func: Custom function to generate cache keys [TODO: Not implemented yet]
    """
    # Mark parameters as unused for now - these are placeholders for future enhancement
    _ = ttl_seconds, memory_only, cache_key_func

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check if caching is disabled
            if kwargs.pop("_no_cache", False):
                return func(*args, **kwargs)

            # Get cached result
            found, result = _cache_manager.get(func.__name__, args, kwargs)
            if found:
                return result

            # Compute and cache result
            result = func(*args, **kwargs)
            _cache_manager.set(func.__name__, args, kwargs, result)
            return result

        # Add cache management methods
        wrapper.clear_cache = lambda: _cache_manager.clear()  # type: ignore
        wrapper.cache_info = lambda: f"Cache dir: {_cache_manager.cache_dir}"  # type: ignore

        return wrapper  # type: ignore

    return decorator


def retry_on_failure(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """Decorator to retry function calls on failure.

    Args:
        max_attempts: Maximum number of attempts
        delay_seconds: Initial delay between attempts
        backoff_multiplier: Multiplier for delay after each failure
        exceptions: Tuple of exceptions to catch and retry
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = delay_seconds

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        # Last attempt, re-raise the exception
                        break

                    # Wait before retrying
                    if delay > 0:
                        time.sleep(delay)
                        delay *= backoff_multiplier

            # All attempts failed, raise the last exception
            raise last_exception

        return wrapper  # type: ignore

    return decorator


def timing_decorator(func: F) -> F:
    """Decorator to measure and log function execution time."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.time()
            duration = end_time - start_time
            print(f"⏱️  {func.__name__} took {duration:.2f}s")

    return wrapper  # type: ignore


def validate_file_exists(func: F) -> F:
    """Decorator to validate that file arguments exist."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Check first positional argument (usually 'path')
        if args and isinstance(args[0], (str, Path)):
            path = Path(args[0])
            if not path.exists():
                raise FileNotFoundError(f"File not found: {path}")

        # Check 'path' keyword argument
        if "path" in kwargs:
            path = Path(kwargs["path"])
            if not path.exists():
                raise FileNotFoundError(f"File not found: {path}")

        return func(*args, **kwargs)

    return wrapper  # type: ignore


# Convenience decorators for common use cases
def cache_expensive_operation(func: F) -> F:
    """Cache results of expensive operations (like parsing large files)."""
    return cache_results(ttl_seconds=3600)(func)  # 1 hour TTL


def retry_gcs_operation(func: F) -> F:
    """Retry GCS operations with exponential backoff."""
    return retry_on_failure(
        max_attempts=3,
        delay_seconds=1.0,
        backoff_multiplier=2.0,
        exceptions=(Exception,),  # Catch all exceptions for GCS
    )(func)


def validate_and_time(func: F) -> F:
    """Combine file validation and timing for parser functions."""
    return timing_decorator(validate_file_exists(func))


# Example usage:
# @cache_expensive_operation
# def parse_large_schema_file(path: str):
#     # Expensive parsing operation
#     pass
#
# @retry_gcs_operation
# def download_from_gcs(gcs_path: str):
#     # GCS download that might fail
#     pass
