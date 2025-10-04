#!/usr/bin/env python3
"""
Tests for decorator patterns.
"""
import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from src.schema_diff.decorators import (
    cache_results,
    retry_on_failure,
    timing_decorator,
    validate_file_exists,
    cache_expensive_operation,
    retry_gcs_operation,
    validate_and_time,
    CacheManager,
)


class TestCacheManager:
    """Test the CacheManager class."""

    def test_cache_manager_creation(self):
        """Test creating a CacheManager."""
        cache_manager = CacheManager()
        assert cache_manager.cache_dir.exists()

    def test_cache_get_set(self):
        """Test basic cache get/set operations."""
        cache_manager = CacheManager()
        
        # Test cache miss
        found, result = cache_manager.get("test_func", ("arg1",), {"kwarg1": "value1"})
        assert not found
        assert result is None
        
        # Test cache set and hit
        test_result = {"data": "test"}
        cache_manager.set("test_func", ("arg1",), {"kwarg1": "value1"}, test_result)
        
        found, result = cache_manager.get("test_func", ("arg1",), {"kwarg1": "value1"})
        assert found
        assert result == test_result

    def test_cache_clear(self):
        """Test clearing cache."""
        cache_manager = CacheManager()
        
        # Set some data
        cache_manager.set("test_func", (), {}, "test_data")
        
        # Verify it's there
        found, _ = cache_manager.get("test_func", (), {})
        assert found
        
        # Clear cache
        cache_manager.clear()
        
        # Verify it's gone
        found, _ = cache_manager.get("test_func", (), {})
        assert not found


class TestCacheDecorator:
    """Test the cache_results decorator."""

    def test_cache_results_basic(self):
        """Test basic caching functionality."""
        # Clear cache to ensure clean test state
        from src.schema_diff.decorators import _cache_manager
        _cache_manager.clear()
        
        call_count = 0
        
        @cache_results()
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # First call
        result1 = expensive_function(5)
        assert result1 == 10
        assert call_count == 1
        
        # Second call with same args (should use cache)
        result2 = expensive_function(5)
        assert result2 == 10
        assert call_count == 1  # Should not increment
        
        # Call with different args
        result3 = expensive_function(10)
        assert result3 == 20
        assert call_count == 2

    def test_cache_results_no_cache_flag(self):
        """Test bypassing cache with _no_cache flag."""
        call_count = 0
        
        @cache_results()
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # Clear any existing cache
        expensive_function.clear_cache()
        
        # First call with _no_cache=True (should not cache)
        result1 = expensive_function(5, _no_cache=True)
        assert result1 == 10
        assert call_count == 1
        
        # Second call with _no_cache=True (should not use cache)
        result2 = expensive_function(5, _no_cache=True)
        assert result2 == 10
        assert call_count == 2  # Should increment
        
        # Third call without _no_cache (should still call function since nothing was cached)
        result3 = expensive_function(5)
        assert result3 == 10
        assert call_count == 3

    def test_cache_expensive_operation(self):
        """Test the cache_expensive_operation convenience decorator."""
        # Clear cache to ensure clean test state
        from src.schema_diff.decorators import _cache_manager
        _cache_manager.clear()
        
        call_count = 0
        
        @cache_expensive_operation
        def parse_large_file(path):
            nonlocal call_count
            call_count += 1
            return f"parsed_{path}"
        
        # First call
        result1 = parse_large_file("test.json")
        assert result1 == "parsed_test.json"
        assert call_count == 1
        
        # Second call (should use cache)
        result2 = parse_large_file("test.json")
        assert result2 == "parsed_test.json"
        assert call_count == 1


class TestRetryDecorator:
    """Test the retry_on_failure decorator."""

    def test_retry_success_first_attempt(self):
        """Test function that succeeds on first attempt."""
        call_count = 0
        
        @retry_on_failure(max_attempts=3)
        def reliable_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = reliable_function()
        assert result == "success"
        assert call_count == 1

    def test_retry_success_after_failures(self):
        """Test function that succeeds after some failures."""
        call_count = 0
        
        @retry_on_failure(max_attempts=3, delay_seconds=0)  # No delay for testing
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary failure")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert call_count == 3

    def test_retry_max_attempts_exceeded(self):
        """Test function that fails all attempts."""
        call_count = 0
        
        @retry_on_failure(max_attempts=2, delay_seconds=0)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError, match="Always fails"):
            always_fails()
        
        assert call_count == 2

    def test_retry_specific_exceptions(self):
        """Test retry only on specific exceptions."""
        call_count = 0
        
        @retry_on_failure(max_attempts=3, delay_seconds=0, exceptions=(ValueError,))
        def selective_retry():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Retry this")
            elif call_count == 2:
                raise RuntimeError("Don't retry this")
            return "success"
        
        with pytest.raises(RuntimeError, match="Don't retry this"):
            selective_retry()
        
        assert call_count == 2

    def test_retry_gcs_operation(self):
        """Test the retry_gcs_operation convenience decorator."""
        call_count = 0
        
        @retry_gcs_operation
        def download_file():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Network error")
            return "downloaded"
        
        result = download_file()
        assert result == "downloaded"
        assert call_count == 2


class TestTimingDecorator:
    """Test the timing_decorator."""

    def test_timing_decorator(self, capsys):
        """Test timing decorator prints execution time."""
        @timing_decorator
        def slow_function():
            time.sleep(0.01)  # 10ms
            return "done"
        
        result = slow_function()
        assert result == "done"
        
        captured = capsys.readouterr()
        assert "slow_function took" in captured.out
        assert "s" in captured.out


class TestValidateFileExists:
    """Test the validate_file_exists decorator."""

    def test_validate_file_exists_success(self):
        """Test validation with existing file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()
            
            @validate_file_exists
            def process_file(path):
                return f"processed {path}"
            
            result = process_file(f.name)
            assert "processed" in result
            
            Path(f.name).unlink()

    def test_validate_file_exists_failure(self):
        """Test validation with non-existent file."""
        @validate_file_exists
        def process_file(path):
            return f"processed {path}"
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            process_file("/nonexistent/file.txt")

    def test_validate_file_exists_keyword_arg(self):
        """Test validation with path as keyword argument."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()
            
            @validate_file_exists
            def process_file(*, path):
                return f"processed {path}"
            
            result = process_file(path=f.name)
            assert "processed" in result
            
            Path(f.name).unlink()


# TestLogCalls removed - log_calls decorator was removed during cleanup


class TestValidateAndTime:
    """Test the validate_and_time composite decorator."""

    def test_validate_and_time_success(self, capsys):
        """Test combined validation and timing."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()
            
            @validate_and_time
            def process_file(path):
                time.sleep(0.01)  # Small delay
                return f"processed {path}"
            
            result = process_file(f.name)
            assert "processed" in result
            
            captured = capsys.readouterr()
            assert "process_file took" in captured.out
            
            Path(f.name).unlink()

    def test_validate_and_time_file_not_found(self):
        """Test validation failure in composite decorator."""
        @validate_and_time
        def process_file(path):
            return f"processed {path}"
        
        with pytest.raises(FileNotFoundError):
            process_file("/nonexistent/file.txt")


class TestDecoratorIntegration:
    """Test decorator combinations and integration."""

    def test_multiple_decorators(self):
        """Test stacking multiple decorators."""
        call_count = 0
        
        @cache_results()
        @retry_on_failure(max_attempts=2, delay_seconds=0)
        def complex_function(x):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First attempt fails")
            return x * 2
        
        # First call (will retry and succeed)
        result1 = complex_function(5)
        assert result1 == 10
        assert call_count == 2
        
        # Second call with same args (should use cache, no retry)
        result2 = complex_function(5)
        assert result2 == 10
        assert call_count == 2  # No additional calls

    def test_decorator_with_kwargs(self):
        """Test decorators preserve function signatures."""
        @cache_results()
        def function_with_kwargs(a, b=None, *args, **kwargs):
            return {"a": a, "b": b, "args": args, "kwargs": kwargs}
        
        result = function_with_kwargs(1, b=2, c=3, extra="value")
        expected = {"a": 1, "b": 2, "args": (), "kwargs": {"c": 3, "extra": "value"}}
        assert result == expected
