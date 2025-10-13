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
import fnmatch
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

__version__ = "1.1.0"


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
    rule_id: Optional[str] = None

    def to_dict(self):
        """Convert to dictionary for JSON output"""
        return asdict(self)


# ==================== HELPER FUNCTIONS ====================

# Node type constants
FUNC_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)
NEST_NODES = (
    ast.If,
    ast.For,
    ast.While,
    ast.With,
    ast.Try,
    getattr(ast, "Match", type(None)),  # Python 3.10+
    getattr(ast, "AsyncWith", type(None)),
    getattr(ast, "AsyncFor", type(None)),
)


def _looks_like_test_file(p: Path) -> bool:
    """Check if file appears to be a test file"""
    s = str(p)
    return (
        any(part in ("tests", "test") for part in p.parts)
        or s.endswith("_test.py")
        or s.startswith("test_")
    )


def _is_meaningful_stmt(s: ast.stmt) -> bool:
    """Check if a statement is meaningful (not pass, ..., or bare return)"""
    if isinstance(s, ast.Pass):
        return False
    if isinstance(s, ast.Expr) and isinstance(getattr(s, "value", None), ast.Constant):
        if getattr(s.value, "value", None) is Ellipsis:
            return False
    if isinstance(s, ast.Return) and s.value is None:
        return False
    return True


def _normalize_args(args: ast.arguments) -> ast.arguments:
    """Normalize function arguments for fingerprinting"""

    def blank(args_list):
        return [
            ast.arg(arg="ARG", annotation=None, type_comment=None) for _ in args_list
        ]

    return ast.arguments(
        posonlyargs=blank(getattr(args, "posonlyargs", [])),
        args=blank(args.args),
        vararg=ast.arg(arg="VARARG", annotation=None) if args.vararg else None,
        kwonlyargs=blank(args.kwonlyargs),
        kw_defaults=[None] * len(args.kwonlyargs),
        kwarg=ast.arg(arg="KWARG", annotation=None) if args.kwarg else None,
        defaults=[None] * len(args.defaults),
    )


class _Normalizer(ast.NodeTransformer):
    """Normalize AST for fingerprinting"""

    def visit_FunctionDef(self, node):
        return self._normalize_fn(node)

    def visit_AsyncFunctionDef(self, node):
        return self._normalize_fn(node)

    def _normalize_fn(self, node):
        # Strip docstring
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(getattr(body[0], "value", None), ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]

        # Clone function header
        ctor = (
            ast.AsyncFunctionDef
            if isinstance(node, ast.AsyncFunctionDef)
            else ast.FunctionDef
        )
        new_node = ctor(
            name="FN",
            args=_normalize_args(node.args),
            body=body or [ast.Pass()],
            decorator_list=[],
            returns=None,
            type_comment=None,
            lineno=0,
            col_offset=0,
        )
        return self.generic_visit(new_node)

    def visit_arg(self, node: ast.arg):
        return ast.copy_location(
            ast.arg(arg="ARG", annotation=None, type_comment=None), node
        )

    def visit_Attribute(self, node: ast.Attribute):
        node = self.generic_visit(node)
        return ast.Attribute(value=node.value, attr="ATTR", ctx=node.ctx)

    def visit_Name(self, node: ast.Name):
        return ast.copy_location(ast.Name(id="NAME", ctx=node.ctx), node)

    def visit_Constant(self, node: ast.Constant):
        if isinstance(node.value, (int, float, complex)):
            return ast.copy_location(ast.Constant(value=0), node)
        if isinstance(node.value, str):
            return ast.copy_location(ast.Constant(value="STR"), node)
        return node


def function_fingerprint(fn: ast.AST) -> str:
    """Create a normalized fingerprint of a function for duplicate detection"""
    # Create a copy to avoid mutating the original
    fn_copy = ast.parse(ast.unparse(fn)).body[0] if hasattr(ast, "unparse") else fn
    norm = _Normalizer().visit(fn_copy)
    norm = ast.fix_missing_locations(norm)
    dump = ast.dump(norm, annotate_fields=False, include_attributes=False)
    return hashlib.md5(dump.encode("utf-8"), usedforsecurity=False).hexdigest()


def _param_count(fn: ast.FunctionDef) -> int:
    """Count total parameters including kwonly, *args, **kwargs"""
    a = fn.args
    count = len(getattr(a, "posonlyargs", [])) + len(a.args) + len(a.kwonlyargs)
    count += 1 if a.vararg else 0
    count += 1 if a.kwarg else 0

    # Ignore self/cls for instance/class methods (but not @staticmethod)
    if (
        a.args
        and a.args[0].arg in ("self", "cls")
        and not any(
            isinstance(d, ast.Name) and d.id == "staticmethod"
            for d in fn.decorator_list
        )
    ):
        count -= 1

    return max(count, 0)


def _loc(node: ast.AST, source: str = "") -> int:
    """Calculate lines of code for a node"""
    if getattr(node, "end_lineno", None):
        return node.end_lineno - node.lineno + 1

    # Fallback to source segment
    if source:
        try:
            seg = ast.get_source_segment(source, node)
            return seg.count("\n") + 1 if seg else 0
        except (TypeError, AttributeError):
            pass

    return 0


def _children_body(n: ast.AST) -> List[ast.stmt]:
    """Extract all statement bodies from a node (including branches)"""
    parts = []
    for attr in ("body", "orelse", "finalbody", "handlers"):
        x = getattr(n, attr, [])
        if isinstance(x, list):
            if attr == "handlers":
                for h in x:
                    parts.extend(getattr(h, "body", []))
            else:
                parts.extend(x)
    return parts


def _max_nesting_in_body(body: List[ast.stmt], depth: int = 0) -> int:
    """Calculate maximum nesting depth in a body of statements"""
    best = depth
    for n in body:
        if isinstance(n, NEST_NODES):
            best = max(best, _max_nesting_in_body(_children_body(n), depth + 1))
        else:
            best = max(best, depth)
    return best


def _match_any(path: Path, globs: List[str]) -> bool:
    """Check if path matches any glob pattern (Windows-safe)"""
    s = path.as_posix()
    return any(fnmatch.fnmatch(s, g) for g in globs or [])


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

    def validate_directory(
        self,
        directory: Path,
        include_globs: Optional[List[str]] = None,
        exclude_globs: Optional[List[str]] = None,
    ) -> List[PatternIssue]:
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
                    ".tox",
                    ".pytest_cache",
                    ".mypy_cache",
                    "htmlcov",
                ]
            )
        ]

        # Apply include/exclude globs
        includes = (self.config.get("include_globs") or []) + (include_globs or [])
        excludes = (self.config.get("exclude_globs") or []) + (exclude_globs or [])

        # Default exclusions for common generated/vendored code
        default_excludes = [
            "**/migrations/**",
            "**/pb2.py",
            "**/pb2_grpc.py",
            "**/*_pb2.py",
            "**/vendor/**",
            "**/vendors/**",
            "**/third_party/**",
        ]
        excludes = list(excludes or []) + default_excludes

        if includes:
            python_files = [f for f in python_files if _match_any(f, includes)]
        if excludes:
            python_files = [f for f in python_files if not _match_any(f, excludes)]

        all_issues = []

        # Single file checks
        for file_path in python_files:
            all_issues.extend(self.validate_file(file_path))

        # Cross-file DRY checks (skip if too many files for performance)
        if 1 < len(python_files) < 500:
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
            issues.extend(self._check_anti_patterns(tree, file_path, content))
            issues.extend(self._check_code_smells(content, file_path))
            issues.extend(self._check_magic_numbers(tree, file_path))
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
        """Check for DRY violations using fingerprint-based detection (O(N))"""
        issues = []

        # Build fingerprint index: {fingerprint: [(file, func_node)]}
        fingerprint_map: Dict[str, List[Tuple[Path, ast.FunctionDef]]] = {}

        for file_path in python_files:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                tree = ast.parse(content)

                for node in ast.walk(tree):
                    if isinstance(node, FUNC_NODES):
                        fp = function_fingerprint(node)
                        if fp not in fingerprint_map:
                            fingerprint_map[fp] = []
                        fingerprint_map[fp].append((file_path, node))
            except (SyntaxError, UnicodeDecodeError):
                # Skip files with syntax errors or encoding issues
                continue

        # Report duplicates within each fingerprint bucket (deduplicated)
        seen_pairs = set()
        for functions in fingerprint_map.values():
            if len(functions) < 2:
                continue

            # Report all duplicates in this bucket
            for i, (file1, func1) in enumerate(functions):
                for file2, func2 in functions[i + 1 :]:
                    # Skip if same function in same file
                    if file1 == file2 and func1.lineno == func2.lineno:
                        continue

                    # Deduplicate symmetric pairs
                    key = tuple(
                        sorted(((str(file1), func1.lineno), (str(file2), func2.lineno)))
                    )
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)

                    issues.append(
                        PatternIssue(
                            category=Category.DRY_VIOLATION.value,
                            severity=Severity.WARNING.value,
                            file_path=str(file1),
                            line_number=func1.lineno,
                            issue_type="duplicate_function",
                            rule_id="DRY001",
                            message=f"Function '{func1.name}' is structurally identical to '{func2.name}' in {file2}",
                            suggestion="Extract common logic into a shared function or use composition",
                        )
                    )

        return issues

    # ==================== ANTI-PATTERN DETECTION ====================

    def _check_anti_patterns(
        self, tree: ast.AST, file_path: Path, content: str = ""
    ) -> List[PatternIssue]:
        """Check for common anti-patterns"""
        issues = []

        for node in ast.walk(tree):
            # God Class detection
            if isinstance(node, ast.ClassDef):
                issues.extend(self._check_god_class(node, file_path, content))

            # Long Method detection (for both sync and async functions)
            elif isinstance(node, FUNC_NODES):
                issues.extend(self._check_long_method(node, file_path, content))
                issues.extend(self._check_too_many_parameters(node, file_path))

                # Skip nesting/magic number checks for test files
                if not _looks_like_test_file(file_path):
                    issues.extend(self._check_deep_nesting_in_function(node, file_path))

        # Empty except blocks (AST-based)
        issues.extend(self._check_empty_except(tree, file_path))

        return issues

    def _check_god_class(
        self, node: ast.ClassDef, file_path: Path, content: str = ""
    ) -> List[PatternIssue]:
        """Check for God Class anti-pattern (too large, too many responsibilities)"""
        issues = []

        # Count methods (both sync and async)
        methods = [n for n in node.body if isinstance(n, FUNC_NODES)]
        method_count = len(methods)

        # Calculate lines of code
        loc = _loc(node, content)

        if loc > self.max_class_lines:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="god_class",
                    rule_id="AP001",
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
                    rule_id="AP002",
                    message=f"Class '{node.name}' has too many methods ({method_count})",
                    suggestion=f"Consider splitting responsibilities (max {self.max_method_count} methods)",
                )
            )

        return issues

    def _check_long_method(
        self, node: ast.FunctionDef, file_path: Path, content: str = ""
    ) -> List[PatternIssue]:
        """Check for Long Method anti-pattern"""
        issues = []

        loc = _loc(node, content)
        if loc > self.max_function_lines:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="long_method",
                    rule_id="AP003",
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

        param_count = _param_count(node)

        if param_count > self.max_parameters:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.WARNING.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="too_many_parameters",
                    rule_id="AP004",
                    message=f"Function '{node.name}' has too many parameters ({param_count})",
                    suggestion=f"Consider using a parameter object or builder pattern (max {self.max_parameters} params)",
                )
            )

        return issues

    def _check_deep_nesting_in_function(
        self, node: ast.FunctionDef, file_path: Path
    ) -> List[PatternIssue]:
        """Check for deeply nested code in a function"""
        issues = []

        depth = _max_nesting_in_body(node.body)

        if depth > self.max_nesting:
            issues.append(
                PatternIssue(
                    category=Category.ANTI_PATTERN.value,
                    severity=Severity.INFO.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="deep_nesting",
                    rule_id="AP005",
                    message=f"Function '{node.name}' has deeply nested code (depth: {depth})",
                    suggestion=f"Reduce nesting using early returns or extraction (max depth {self.max_nesting})",
                )
            )

        return issues

    def _check_empty_except(self, tree: ast.AST, file_path: Path) -> List[PatternIssue]:
        """Check for empty except blocks using AST"""
        issues = []
        for h in [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]:
            if not any(_is_meaningful_stmt(b) for b in h.body):
                issues.append(
                    PatternIssue(
                        category=Category.CODE_SMELL.value,
                        severity=Severity.WARNING.value,
                        file_path=str(file_path),
                        line_number=h.lineno,
                        issue_type="empty_except",
                        rule_id="CS001",
                        message="Empty except block swallows errors",
                        suggestion="At minimum, log the exception or re-raise a contextual error",
                    )
                )
        return issues

    # ==================== CODE SMELL DETECTION ====================

    def _check_code_smells(self, content: str, file_path: Path) -> List[PatternIssue]:
        """Check for common code smells (text-based patterns)"""
        issues = []
        lines = content.split("\n")

        # Skip noisy checks for test/example files
        is_test_or_example = _looks_like_test_file(file_path) or any(
            part in file_path.parts for part in ["examples", "example", "docs"]
        )

        for i, line in enumerate(lines, 1):
            # Skip if line has ignore marker
            if "# noqa" in line or "# nosec" in line:
                continue

            # Commented-out code (less noisy - only flag obvious cases)
            if not is_test_or_example:
                stripped = line.strip()
                # Only flag multi-statement or def/class (not simple assignments/imports in comments)
                if stripped.startswith("#") and re.search(
                    r"#\s*(def\s+\w+|class\s+\w+|if\s+\w+.*:|for\s+\w+.*:|while\s+\w+.*:)\s*",
                    line,
                ):
                    issues.append(
                        PatternIssue(
                            category=Category.CODE_SMELL.value,
                            severity=Severity.INFO.value,
                            file_path=str(file_path),
                            line_number=i,
                            issue_type="commented_code",
                            rule_id="CS002",
                            message="Commented-out code detected",
                            suggestion="Remove dead code or use version control",
                        )
                    )

            # TODO/FIXME markers (only flag FIXME and XXX as issues, TODO is acceptable)
            if not is_test_or_example:
                if re.search(r"#\s*(FIXME|XXX|HACK)\b", line, re.IGNORECASE):
                    marker = re.search(
                        r"#\s*(FIXME|XXX|HACK)\b", line, re.IGNORECASE
                    ).group(1)
                    issues.append(
                        PatternIssue(
                            category=Category.CODE_SMELL.value,
                            severity=Severity.INFO.value,
                            file_path=str(file_path),
                            line_number=i,
                            issue_type="todo_marker",
                            rule_id="CS003",
                            message=f"{marker} comment found - requires attention",
                            suggestion="Address urgent markers before production",
                        )
                    )

        return issues

    def _check_magic_numbers(
        self, tree: ast.AST, file_path: Path
    ) -> List[PatternIssue]:
        """Check for magic numbers using AST (more accurate than regex)"""
        # Skip test files
        if _looks_like_test_file(file_path):
            return []

        issues = []
        ALLOW = {-1, 0, 1, 2}

        def _is_constant_assignment(parent_stack) -> bool:
            """Check if number is assigned to ALL_CAPS constant"""
            for i in range(len(parent_stack) - 1):
                n = parent_stack[i]
                ch = parent_stack[i + 1]
                if isinstance(n, (ast.Module, ast.ClassDef)) and isinstance(
                    ch, ast.Assign
                ):
                    for t in ch.targets:
                        if isinstance(t, ast.Name) and t.id.isupper():
                            return True
            return False

        class MagicNumberVisitor(ast.NodeVisitor):
            def __init__(self):
                self.hits = []
                self.stack = []

            def generic_visit(self, node):
                self.stack.append(node)
                super().generic_visit(node)
                self.stack.pop()

            def visit_Constant(self, node: ast.Constant):
                if isinstance(node.value, (int, float)) and node.value not in ALLOW:
                    if not _is_constant_assignment(self.stack):
                        self.hits.append(node)
                self.generic_visit(node)

        visitor = MagicNumberVisitor()
        visitor.visit(tree)

        for node in visitor.hits:
            issues.append(
                PatternIssue(
                    category=Category.CODE_SMELL.value,
                    severity=Severity.INFO.value,
                    file_path=str(file_path),
                    line_number=node.lineno,
                    issue_type="magic_number",
                    rule_id="CS004",
                    message=f"Magic number {node.value} detected",
                    suggestion="Extract a named constant (e.g., MAX_RETRIES = 10)",
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
    parser.add_argument(
        "--include", action="append", help="Glob pattern(s) to include (repeatable)"
    )
    parser.add_argument(
        "--exclude", action="append", help="Glob pattern(s) to exclude (repeatable)"
    )
    parser.add_argument(
        "--fail-on",
        choices=["error", "warning", "info"],
        default="error",
        help="Exit with error if issues at this severity or higher are found (default: error)",
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
        issues = validator.validate_directory(path, args.include, args.exclude)

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
            "tool": {
                "name": "validate_patterns",
                "version": __version__,
                "python": sys.version.split()[0],
            },
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

    # Exit with error code based on --fail-on threshold
    rank = {"error": 3, "warning": 2, "info": 1}
    threshold = rank[args.fail_on]
    max_seen = max([rank.get(i.severity, 0) for i in issues], default=0)
    sys.exit(1 if max_seen >= threshold else 0)


if __name__ == "__main__":
    main()
