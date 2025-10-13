#!/usr/bin/env python3
"""
Unit tests for validate_patterns.py
"""

import tempfile
import unittest
from pathlib import Path

from validate_patterns import (
    Language,
    PatternIssue,
    PatternType,
    PatternValidator,
    detect_language,
    get_source_files,
)


class TestPatternValidator(unittest.TestCase):
    """Test the PatternValidator class"""

    def setUp(self):
        self.validator = PatternValidator(Language.PYTHON)

    def test_factory_pattern_detection(self):
        """Test factory pattern issue detection"""
        content = """
class UserFactory:
    def create_user(self):
        return AdminUser()  # Should be flagged
"""
        issues = self.validator._validate_factory_pattern("test.py", content)
        self.assertTrue(len(issues) > 0)
        self.assertEqual(issues[0].pattern, PatternType.FACTORY)
        self.assertEqual(issues[0].issue_type, "concrete_return")

    def test_builder_pattern_detection(self):
        """Test builder pattern issue detection"""
        content = """
class QueryBuilder:
    def select(self, fields):
        self.query = f"SELECT {fields}"
        # Missing return self - no chaining detected
"""
        issues = self.validator._validate_builder_pattern("test.py", content)
        self.assertTrue(len(issues) > 0)
        # Should detect missing method chaining and missing build method

    def test_decorator_pattern_detection(self):
        """Test decorator pattern issue detection"""
        content = """
class LoggingDecorator:
    def __init__(self, component):
        self.component = component

    def operation(self):
        return "hardcoded"  # No delegation
"""
        issues = self.validator._validate_decorator_pattern("test.py", content)
        self.assertTrue(len(issues) > 0)
        self.assertEqual(issues[0].pattern, PatternType.DECORATOR)
        self.assertEqual(issues[0].issue_type, "no_delegation")

    def test_observer_pattern_detection(self):
        """Test observer pattern issue detection"""
        content = """
class EventSubject:
    def __init__(self):
        self.state = None
        # Missing observer list and notify method
"""
        issues = self.validator._validate_observer_pattern("test.py", content)
        # Should detect missing observer list and notify method
        self.assertTrue(len(issues) > 0)


class TestLanguageDetection(unittest.TestCase):
    """Test language auto-detection"""

    def test_python_detection(self):
        """Test Python language detection"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Python files
            (Path(tmpdir) / "test1.py").write_text("print('hello')")
            (Path(tmpdir) / "test2.py").write_text("def func(): pass")

            language = detect_language(tmpdir)
            self.assertEqual(language, Language.PYTHON)

    def test_java_detection(self):
        """Test Java language detection"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Java files
            (Path(tmpdir) / "Test.java").write_text("public class Test {}")

            language = detect_language(tmpdir)
            self.assertEqual(language, Language.JAVA)

    def test_javascript_detection(self):
        """Test JavaScript language detection"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create JavaScript files
            (Path(tmpdir) / "test.js").write_text("console.log('hello');")

            language = detect_language(tmpdir)
            self.assertEqual(language, Language.JAVASCRIPT)

    def test_no_language_detection(self):
        """Test when no language can be detected"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create non-source files
            (Path(tmpdir) / "readme.txt").write_text("Hello world")

            language = detect_language(tmpdir)
            self.assertIsNone(language)


class TestSourceFileCollection(unittest.TestCase):
    """Test source file collection"""

    def test_python_file_collection(self):
        """Test collecting Python source files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Python files
            (Path(tmpdir) / "test1.py").write_text("print('hello')")
            (Path(tmpdir) / "test2.py").write_text("def func(): pass")
            # Create non-Python file
            (Path(tmpdir) / "readme.txt").write_text("Hello")

            files = get_source_files(tmpdir, Language.PYTHON)
            py_files = [f for f in files if f.endswith(".py")]
            self.assertEqual(len(py_files), 2)

    def test_directory_filtering(self):
        """Test that common non-source directories are filtered"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source file
            (Path(tmpdir) / "test.py").write_text("print('hello')")

            # Create files in filtered directories
            node_modules = Path(tmpdir) / "node_modules"
            node_modules.mkdir()
            (node_modules / "package.py").write_text("# should be ignored")

            pycache = Path(tmpdir) / "__pycache__"
            pycache.mkdir()
            (pycache / "cached.py").write_text("# should be ignored")

            files = get_source_files(tmpdir, Language.PYTHON)
            # Should only find the main test.py, not the ones in filtered dirs
            self.assertEqual(len(files), 1)
            self.assertTrue(files[0].endswith("test.py"))


class TestPatternIssue(unittest.TestCase):
    """Test PatternIssue data structure"""

    def test_pattern_issue_creation(self):
        """Test creating a PatternIssue"""
        issue = PatternIssue(
            pattern=PatternType.FACTORY,
            file_path="test.py",
            line_number=10,
            issue_type="concrete_return",
            description="Factory returns concrete class",
            severity="warning",
        )

        self.assertEqual(issue.pattern, PatternType.FACTORY)
        self.assertEqual(issue.file_path, "test.py")
        self.assertEqual(issue.line_number, 10)
        self.assertEqual(issue.issue_type, "concrete_return")
        self.assertEqual(issue.severity, "warning")


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete workflow"""

    def test_end_to_end_validation(self):
        """Test complete validation workflow"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a Python file with pattern issues
            test_file = Path(tmpdir) / "test.py"
            test_file.write_text(
                """
class UserFactory:
    def create_user(self):
        return AdminUser()  # Issue: concrete return

class QueryBuilder:
    def select(self, fields):
        self.query = f"SELECT {fields}"
        # Issue: no return self
"""
            )

            validator = PatternValidator(Language.PYTHON)
            issues = validator.validate_file(str(test_file))

            # Should find at least the factory issue
            self.assertTrue(len(issues) > 0)
            factory_issues = [i for i in issues if i.pattern == PatternType.FACTORY]
            self.assertTrue(len(factory_issues) > 0)


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
