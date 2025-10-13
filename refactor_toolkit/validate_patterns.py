#!/usr/bin/env python3
"""
Enhanced Pattern and Anti-Pattern Validator for Python

Detects DRY violations, anti-patterns, code smells, and design pattern issues.
Provides JSON output compatible with validate.py.

Usage:
    python validate_patterns.py /path/to/src --json
    python validate_patterns.py /path/to/src --config pattern_config.json
"""

import argparse
import ast
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


class Severity(Enum):
    """Issue severity levels"""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Category(Enum):
    """Issue categories"""

    DRY_VIOLATION = "dry_violation"
    ANTI_PATTERN = "anti_pattern"
    CODE_SMELL = "code_smell"
    DESIGN_PATTERN = "design_pattern"


@dataclass
class PatternIssue:
    """Represents a detected pattern issue"""

    category: str
    severity: str
    file_path: str
    line_number: int
    issue_type: str
    message: str
    suggestion: Optional[str] = None

    def to_dict(self):
        """Convert to dictionary for JSON output"""
        return asdict(self)


class PatternValidator:
    """Main validator for patterns, anti-patterns, and code quality in Python"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.issues: List[PatternIssue] = []

        # Thresholds (can be overridden by config)
        self.max_function_lines = self.config.get("max_function_lines", 50)
        self.max_class_lines = self.config.get("max_class_lines", 500)
        self.max_method_count = self.config.get("max_method_count", 20)
        self.max_parameters = self.config.get("max_parameters", 5)
        self.max_nesting = self.config.get("max_nesting", 4)
        self.dry_similarity_threshold = self.config.get(
            "dry_similarity_threshold", 0.85
        )

    def validate_directory(self, directory: Path) -> List[PatternIssue]:
        """Validate all Python files in a directory"""
        python_files = list(directory.rglob("*.py"))

        # Filter out virtual environments and common exclusions
        python_files = [
            f
            for f in python_files
            if not any(
                part in f.parts
                for part in [
                    "venv",
                    ".venv",
                    "site-packages",
                    "__pycache__",
                    "node_modules",
                    ".git",
                    "build",
                    "dist",
                ]
            )
        ]

        all_issues = []

        # Single file checks
        for file_path in python_files:
            all_issues.extend(self.validate_file(file_path))

        # Cross-file DRY checks
        if len(python_files) > 1:
            all_issues.extend(self._check_dry_violations(python_files))

        return all_issues

    def validate_file(self, file_path: Path) -> List[PatternIssue]:
        """Validate a single Python file"""
        if not file_path.exists():
            return []

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception as e:
            return [
                PatternIssue(
                    category=Category.CODE_SMELL.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=0,
                    issue_type="file_read_error",
                    message=f"Could not read file: {e}",
                    suggestion="Check file permissions and encoding",
                )
            ]

        issues = []

        # Parse AST for deeper analysis
        try:
            tree = ast.parse(content)
            issues.extend(self._check_anti_patterns(tree, file_path))
            issues.extend(self._check_code_smells(content, file_path))
        except SyntaxError as e:
            issues.append(
                PatternIssue(
                    category=Category.CODE_SMELL.value,
                    severity=Severity.ERROR.value,
                    file_path=str(file_path),
                    line_number=e.lineno or 0,
                    issue_type="syntax_error",
                    message=f"Syntax error: {e.msg}",
                    suggestion="Fix syntax error before running pattern validation",
                )
            )

        return issues

    # ==================== DRY VIOLATION DETECTION ====================

    def _check_dry_violations(self, python_files: List[Path]) -> List[PatternIssue]:
        """Check for DRY violations across multiple files"""
        issues = []

        # Extract all functions from all files
        all_functions = []
        for file_path in python_files:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                tree = ast.parse(content)

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        all_functions.append((file_path, node))
            except:
                continue

        # Compare functions for similarity
        for i, (file1, func1) in enumerate(all_functions):
            for file2, func2 in all_functions[i + 1 :]:
                similarity = self._calculate_function_similarity(func1, func2)

                if similarity >= self.dry_similarity_threshold:
                    issues.append(
                        PatternIssue(
                            category=Category.DRY_VIOLATION.value,
                            severity=Severity.WARNING.value,
                            file_path=str(file1),
                            line_number=func1.lineno,
                            issue_type="duplicate_function",
                            message=f"Function '{func1.name}' is {similarity*100:.0f}% similar to '{func2.name}' in {file2}",
                            suggestion="Consider extracting common logic into a shared function",
                        )
                    )

        return issues

    def _calculate_function_similarity(
        self, func1: ast.FunctionDef, func2: ast.FunctionDef
    ) -> float:
        """Calculate similarity between two functions using AST comparison"""
        # Get normalized AST dumps
        dump1 = self._normalize_ast_for_comparison(func1)
        dump2 = self._normalize_ast_for_comparison(func2)

        dump1 = dump1.strip()
        dump2 = dump2.strip()

        if not dump1 or not dump2:
            return 0.0

        # Simple similarity based on normalized structure
        common = len(set(dump1.split()) & set(dump2.split()))
        total = len(set(dump1.split()) | set(dump2.split()))

        return common / total if total > 0 else 0.0

    def _normalize_ast_for_comparison(self, node: ast.AST) -> str:
        """Normalize AST by removing variable names for comparison"""
        # Get AST dump and replace variable names with VAR
        dump = ast.dump(node)
        # Replace specific identifiers with generic VAR
        normalized = re.sub(r"id='[^']*'", "id='VAR'", dump)
        normalized = re.sub(r's="[^"]*"', 's="STR"', normalized)
        normalized = re.sub(r"n=\d+", "n=NUM", normalized)
        return normalized

    # ==================== ANTI-PATTERN DETECTION ====================

    def _check_anti_patterns(
        self, tree: ast.AST, file_path: Path
    ) -> List[PatternIssue]:
        """Check for common anti-patterns"""
        issues = []

        for node in ast.walk(tree):
            # God Class detection
            if isinstance(node, ast.ClassDef):
                issues.extend(self._check_god_class(node, file_path))

            # Long Method detection
            elif isinstance(node, ast.FunctionDef):
                issues.extend(self._check_long_method(node, file_path))
                issues.extend(self._check_too_many_parameters(node, file_path))

            # Deep nesting detection
            if isinstance(node, (ast.If, ast.For, ast.While, ast.With)):
                issues.extend(self._check_deep_nesting(node, file_path))

        return issues

    def _check_god_class(
        self, node: ast.ClassDef, file_path: Path
    ) -> List[PatternIssue]:
        """Check for God Class anti-pattern (too large, too many responsibilities)"""
        issues = []

        # Count methods
        methods = [n for n in node.body if isinstance(n, ast.FunctionDef)]
        method_count = len(methods)

        # Estimate lines of code
        if hasattr(node, "end_lineno") and node.end_lineno:
            loc = node.end_lineno - node.lineno
        else:
            loc = len(methods) * 10  # Rough estimate

        if loc > self.max_class_lines:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="god_class",
                    message=f"Class '{node.name}' is too large ({loc} lines)",
                    suggestion=f"Consider splitting into smaller classes (max {self.max_class_lines} lines)",
                )
            )

        if method_count > self.max_method_count:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="too_many_methods",
                    message=f"Class '{node.name}' has too many methods ({method_count})",
                    suggestion=f"Consider splitting responsibilities (max {self.max_method_count} methods)",
                )
            )

        return issues

    def _check_long_method(
        self, node: ast.FunctionDef, file_path: Path
    ) -> List[PatternIssue]:
        """Check for Long Method anti-pattern"""
        issues = []

        if hasattr(node, "end_lineno") and node.end_lineno:
            loc = node.end_lineno - node.lineno

            if loc > self.max_function_lines:
                issues.append(
                    PatternIssue(
                        category=Category.ANTI_PATTERN.value,
                        severity=Severity.WARNING.value,
                        file_path=str(file_path),
                        line_number=node.lineno,
                        issue_type="long_method",
                        message=f"Function '{node.name}' is too long ({loc} lines)",
                        suggestion=f"Break into smaller functions (max {self.max_function_lines} lines)",
                    )
                )

        return issues

    def _check_too_many_parameters(
        self, node: ast.FunctionDef, file_path: Path
    ) -> List[PatternIssue]:
        """Check for too many function parameters"""
        issues = []

        param_count = len(node.args.args)
        # Don't count 'self' or 'cls'
        if param_count > 0 and node.args.args[0].arg in ("self", "cls"):
            param_count -= 1

        if param_count > self.max_parameters:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="too_many_parameters",
                    message=f"Function '{node.name}' has too many parameters ({param_count})",
                    suggestion=f"Consider using a parameter object or builder pattern (max {self.max_parameters} params)",
                )
            )

        return issues

    def _check_deep_nesting(self, node: ast.AST, file_path: Path) -> List[PatternIssue]:
        """Check for deeply nested code"""
        issues = []

        depth = self._calculate_nesting_depth(node)

        if depth > self.max_nesting:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.INFO.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="deep_nesting",
                    message=f"Deeply nested code (depth: {depth})",
                    suggestion=f"Reduce nesting using early returns or extraction (max depth {self.max_nesting})",
                )
            )

        return issues

    def _calculate_nesting_depth(self, node: ast.AST, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth of a node"""
        max_depth = current_depth

        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.If, ast.For, ast.While, ast.With, ast.Try)):
                child_depth = self._calculate_nesting_depth(child, current_depth + 1)
                max_depth = max(max_depth, child_depth)

        return max_depth

    # ==================== CODE SMELL DETECTION ====================

    def _check_code_smells(self, content: str, file_path: Path) -> List[PatternIssue]:
        """Check for common code smells"""
        issues = []
        lines = content.split("\n")

        for i, line in enumerate(lines, 1):
            # Match any integer except -1, 0, 1, 2
            if re.search(
                r"\b(?!(?:-?1|0|2)\b)-?\d+\b", line
            ) and not line.strip().startswith("#"):
                issues.append(
                    PatternIssue(
                        category=Category.CODE_SMELL.value,
                        severity=Severity.INFO.value,
                        file_path=str(file_path),
                        line_number=i,
                        issue_type="magic_number",
                        message="Magic number detected - consider using a named constant",
                        suggestion="Replace with a descriptive constant name",
                    )
                )

            # Empty except blocks
            if re.search(r"except.*:\s*pass\s*$", line):
                issues.append(
                    PatternIssue(
                        category=Category.CODE_SMELL.value,
                        severity=Severity.WARNING.value,
                        file_path=str(file_path),
                        line_number=i,
                        issue_type="empty_except",
                        message="Empty except block - errors silently ignored",
                        suggestion="Log the error or handle it explicitly",
                    )
                )

            # Commented-out code (line starts with # and looks like code)
            stripped = line.strip()
            if stripped.startswith("#") and (
                re.search(r"#\s*(def|class|import|from|if|for|while|return)\s+", line)
                or re.search(r"#\s*\w+\s*=\s*", line)
            ):
                issues.append(
                    PatternIssue(
                        category=Category.CODE_SMELL.value,
                        severity=Severity.INFO.value,
                        file_path=str(file_path),
                        line_number=i,
                        issue_type="commented_code",
                        message="Commented-out code detected",
                        suggestion="Remove dead code or use version control",
                    )
                )

            # TODO/FIXME markers
            if re.search(r"#\s*(TODO|FIXME|XXX|HACK)", line, re.IGNORECASE):
                marker = re.search(
                    r"#\s*(TODO|FIXME|XXX|HACK)", line, re.IGNORECASE
                ).group(1)
                issues.append(
                    PatternIssue(
                        category=Category.CODE_SMELL.value,
                        severity=Severity.INFO.value,
                        file_path=str(file_path),
                        line_number=i,
                        issue_type="todo_marker",
                        message=f"{marker} comment found",
                        suggestion="Address or remove TODO markers before production",
                    )
                )

        return issues


def load_config(config_path: Optional[Path]) -> Dict:
    """Load configuration from JSON file"""
    if not config_path or not config_path.exists():
        return {}

    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load config file: {e}", file=sys.stderr)
        return {}


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced Pattern and Anti-Pattern Validator for Python"
    )
    parser.add_argument("path", type=str, help="Path to directory or file to validate")
    parser.add_argument("--config", type=str, help="Path to configuration file (JSON)")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument(
        "--severity",
        type=str,
        choices=["error", "warning", "info"],
        help="Minimum severity to report",
    )

    args = parser.parse_args()

    # Load configuration
    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)

    # Create validator
    validator = PatternValidator(config=config)

    # Validate path
    path = Path(args.path)
    if not path.exists():
        print(f"Error: Path '{path}' does not exist", file=sys.stderr)
        sys.exit(1)

    # Run validation
    if path.is_file():
        issues = validator.validate_file(path)
    else:
        issues = validator.validate_directory(path)

    # Filter by severity if specified
    if args.severity:
        severity_order = {"error": 3, "warning": 2, "info": 1}
        min_level = severity_order.get(args.severity, 1)
        issues = [
            issue
            for issue in issues
            if severity_order.get(issue.severity, 1) >= min_level
        ]

    # Output results
    if args.json:
        output = {
            "total_issues": len(issues),
            "by_severity": {
                "error": len([i for i in issues if i.severity == Severity.ERROR.value]),
                "warning": len(
                    [i for i in issues if i.severity == Severity.WARNING.value]
                ),
                "info": len([i for i in issues if i.severity == Severity.INFO.value]),
            },
            "by_category": {
                "dry_violation": len(
                    [i for i in issues if i.category == Category.DRY_VIOLATION.value]
                ),
                "anti_pattern": len(
                    [i for i in issues if i.category == Category.ANTI_PATTERN.value]
                ),
                "code_smell": len(
                    [i for i in issues if i.category == Category.CODE_SMELL.value]
                ),
                "design_pattern": len(
                    [i for i in issues if i.category == Category.DESIGN_PATTERN.value]
                ),
            },
            "issues": [issue.to_dict() for issue in issues],
        }
        print(json.dumps(output, indent=2))
    else:
        # Human-readable output
        if not issues:
            print("✅ No pattern issues found!")
        else:
            print(f"\n🔍 Found {len(issues)} pattern issue(s):\n")

            sev_order = {"error": 0, "warning": 1, "info": 2}
            for issue in sorted(
                issues,
                key=lambda x: (
                    sev_order.get(x.severity, 2),
                    x.file_path,
                    x.line_number,
                ),
            ):
                severity_icon = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}.get(
                    issue.severity, "•"
                )

                print(f"{severity_icon} {issue.file_path}:{issue.line_number}")
                print(f"   [{issue.category}] {issue.issue_type}")
                print(f"   {issue.message}")
                if issue.suggestion:
                    print(f"   💡 {issue.suggestion}")
                print()

    # Exit with error code if there are errors
    error_count = len([i for i in issues if i.severity == Severity.ERROR.value])
    sys.exit(1 if error_count > 0 else 0)


if __name__ == "__main__":
    main()
