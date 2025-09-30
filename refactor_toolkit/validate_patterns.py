#!/usr/bin/env python3
"""
Universal Design Pattern Validator

A language-agnostic tool to validate common design patterns in any codebase.
Supports Python, Java, JavaScript/TypeScript, Go, and more.

Usage:
    python validate_patterns.py /path/to/src --language python
    python validate_patterns.py /path/to/src --language java --patterns factory,builder
    python validate_patterns.py /path/to/src --auto-detect
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional


class Language(Enum):
    PYTHON = "python"
    JAVA = "java"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    GO = "go"
    RUST = "rust"
    CSHARP = "csharp"


class PatternType(Enum):
    FACTORY = "factory"
    BUILDER = "builder"
    DECORATOR = "decorator"
    OBSERVER = "observer"
    STRATEGY = "strategy"
    REPOSITORY = "repository"
    SINGLETON = "singleton"


@dataclass
class PatternIssue:
    pattern: PatternType
    file_path: str
    line_number: int
    issue_type: str
    description: str
    severity: str  # "error", "warning", "info"


class PatternValidator:
    """Base class for pattern validation"""

    def __init__(self, language: Language):
        self.language = language
        self.issues: List[PatternIssue] = []

    def validate_file(self, file_path: str) -> List[PatternIssue]:
        """Validate patterns in a single file"""
        if not os.path.exists(file_path):
            return []

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        file_issues = []
        file_issues.extend(self._validate_factory_pattern(file_path, content))
        file_issues.extend(self._validate_builder_pattern(file_path, content))
        file_issues.extend(self._validate_decorator_pattern(file_path, content))
        file_issues.extend(self._validate_observer_pattern(file_path, content))
        file_issues.extend(self._validate_strategy_pattern(file_path, content))
        file_issues.extend(self._validate_repository_pattern(file_path, content))

        return file_issues

    def _validate_factory_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Factory pattern implementation"""
        issues = []

        # Language-specific factory pattern checks
        if self.language == Language.PYTHON:
            issues.extend(self._check_python_factory(file_path, content))
        elif self.language == Language.JAVA:
            issues.extend(self._check_java_factory(file_path, content))
        elif self.language in [Language.JAVASCRIPT, Language.TYPESCRIPT]:
            issues.extend(self._check_js_factory(file_path, content))

        return issues

    def _check_python_factory(self, file_path: str, content: str) -> List[PatternIssue]:
        """Check Python factory patterns"""
        issues = []
        lines = content.split("\n")

        # Check for factory classes
        factory_classes = []
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*Factory\w*", line):
                factory_classes.append((i + 1, line.strip()))

        # Check if factory methods return concrete types
        for line_num, _factory_line in factory_classes:
            # Look for methods in this factory
            for j in range(line_num, min(line_num + 50, len(lines))):
                line = lines[j]
                if re.search(r"return\s+\w+\(", line) and not re.search(
                    r"return\s+self\.|return\s+cls\.", line
                ):
                    # Check if it's returning a concrete instantiation
                    if re.search(r"return\s+[A-Z]\w*\(", line):
                        issues.append(
                            PatternIssue(
                                pattern=PatternType.FACTORY,
                                file_path=file_path,
                                line_number=j + 1,
                                issue_type="concrete_return",
                                description=f"Factory method returns concrete class instead of interface: {line.strip()}",
                                severity="warning",
                            )
                        )

        # Check for direct instantiation bypassing factory
        for i, line in enumerate(lines):
            # Look for direct instantiation of classes that might have factories
            if re.search(r"\w+\s*=\s*[A-Z]\w*\(", line):
                class_name = re.search(r"([A-Z]\w*)\(", line)
                if class_name:
                    # Check if there's a corresponding factory
                    factory_pattern = f"{class_name.group(1)}Factory"
                    if factory_pattern.lower() in content.lower():
                        issues.append(
                            PatternIssue(
                                pattern=PatternType.FACTORY,
                                file_path=file_path,
                                line_number=i + 1,
                                issue_type="bypass_factory",
                                description=f"Direct instantiation found, consider using factory: {line.strip()}",
                                severity="info",
                            )
                        )

        return issues

    def _check_java_factory(self, file_path: str, content: str) -> List[PatternIssue]:
        """Check Java factory patterns"""
        issues = []
        lines = content.split("\n")

        # Check for factory classes
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*Factory\w*", line):
                # Check methods in factory class
                for j in range(i, min(i + 100, len(lines))):
                    method_line = lines[j]
                    # Check if factory method returns concrete type
                    if re.search(r"return\s+new\s+[A-Z]\w*\(", method_line):
                        issues.append(
                            PatternIssue(
                                pattern=PatternType.FACTORY,
                                file_path=file_path,
                                line_number=j + 1,
                                issue_type="concrete_return",
                                description=f"Factory returns concrete class: {method_line.strip()}",
                                severity="warning",
                            )
                        )

        return issues

    def _check_js_factory(self, file_path: str, content: str) -> List[PatternIssue]:
        """Check JavaScript/TypeScript factory patterns"""
        issues = []
        lines = content.split("\n")

        # Check for factory functions/classes
        for i, line in enumerate(lines):
            if re.search(r"(function|class)\s+\w*[Ff]actory\w*|create\w+Factory", line):
                # Look for return statements
                for j in range(i, min(i + 50, len(lines))):
                    return_line = lines[j]
                    if re.search(r"return\s+new\s+[A-Z]\w*\(", return_line):
                        issues.append(
                            PatternIssue(
                                pattern=PatternType.FACTORY,
                                file_path=file_path,
                                line_number=j + 1,
                                issue_type="concrete_return",
                                description=f"Factory returns concrete class: {return_line.strip()}",
                                severity="warning",
                            )
                        )

        return issues

    def _validate_builder_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Builder pattern implementation"""
        issues = []
        lines = content.split("\n")

        # Look for builder classes
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*Builder\w*", line):
                # Check for method chaining
                has_chaining = False
                has_build_method = False

                for j in range(i, min(i + 100, len(lines))):
                    method_line = lines[j]

                    # Check for method chaining (return this/self)
                    if re.search(r"return\s+(this|self)", method_line):
                        has_chaining = True

                    # Check for build method
                    if re.search(
                        r"def\s+build\(|build\s*\(|function\s+build\(", method_line
                    ):
                        has_build_method = True

                if not has_chaining:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.BUILDER,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_chaining",
                            description="Builder class found but no method chaining detected",
                            severity="warning",
                        )
                    )

                if not has_build_method:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.BUILDER,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_build_method",
                            description="Builder class found but no build() method detected",
                            severity="error",
                        )
                    )

        return issues

    def _validate_decorator_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Decorator pattern implementation"""
        issues = []
        lines = content.split("\n")

        # Look for decorator classes
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*Decorator\w*", line):
                has_delegation = False

                # Check for delegation to wrapped component
                for j in range(i, min(i + 100, len(lines))):
                    delegate_line = lines[j]
                    if re.search(
                        r"\.(component|wrapped|delegate)\.|self\._\w+\.", delegate_line
                    ):
                        has_delegation = True
                        break

                if not has_delegation:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.DECORATOR,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_delegation",
                            description="Decorator class found but no delegation to wrapped component detected",
                            severity="warning",
                        )
                    )

        return issues

    def _validate_observer_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Observer pattern implementation"""
        issues = []
        lines = content.split("\n")

        # Look for observer/subject classes
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*(Subject|Observable)\w*", line):
                has_observer_list = False
                has_notify_method = False

                for j in range(i, min(i + 100, len(lines))):
                    check_line = lines[j]

                    # Check for observer list/collection
                    if re.search(
                        r"(observers|listeners|subscribers)", check_line.lower()
                    ):
                        has_observer_list = True

                    # Check for notify method
                    if re.search(r"def\s+(notify|update|fire|trigger)", check_line):
                        has_notify_method = True

                if not has_observer_list:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.OBSERVER,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_observer_list",
                            description="Subject class found but no observer collection detected",
                            severity="warning",
                        )
                    )

                if not has_notify_method:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.OBSERVER,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_notify_method",
                            description="Subject class found but no notify method detected",
                            severity="warning",
                        )
                    )

        return issues

    def _validate_strategy_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Strategy pattern implementation"""
        issues = []
        lines = content.split("\n")

        # Look for strategy interfaces/classes
        for i, line in enumerate(lines):
            if re.search(r"(interface|class)\s+\w*Strategy\w*", line):
                # Check if there's a context class that uses strategies
                has_context_usage = False

                for _j, context_line in enumerate(lines):
                    if re.search(
                        r"(setStrategy|set_strategy|changeStrategy)", context_line
                    ):
                        has_context_usage = True
                        break

                if not has_context_usage:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.STRATEGY,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_runtime_switching",
                            description="Strategy interface found but no runtime strategy switching detected",
                            severity="info",
                        )
                    )

        return issues

    def _validate_repository_pattern(
        self, file_path: str, content: str
    ) -> List[PatternIssue]:
        """Validate Repository pattern implementation"""
        issues = []
        lines = content.split("\n")

        # Look for repository classes
        for i, line in enumerate(lines):
            if re.search(r"class\s+\w*Repository\w*", line):
                has_crud_methods = False
                uses_domain_objects = True

                crud_patterns = [
                    r"find",
                    r"save",
                    r"delete",
                    r"get",
                    r"create",
                    r"update",
                ]

                for j in range(i, min(i + 100, len(lines))):
                    method_line = lines[j]

                    # Check for CRUD methods
                    for pattern in crud_patterns:
                        if re.search(f"def\\s+{pattern}|{pattern}\\s*\\(", method_line):
                            has_crud_methods = True
                            break

                    # Check for DTO/Entity usage (anti-pattern in domain)
                    if re.search(r"(DTO|Entity|Model)(?!Interface)", method_line):
                        uses_domain_objects = False

                if not has_crud_methods:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.REPOSITORY,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="no_crud_methods",
                            description="Repository class found but no CRUD methods detected",
                            severity="warning",
                        )
                    )

                if not uses_domain_objects:
                    issues.append(
                        PatternIssue(
                            pattern=PatternType.REPOSITORY,
                            file_path=file_path,
                            line_number=i + 1,
                            issue_type="dto_usage",
                            description="Repository uses DTOs/Entities instead of domain objects",
                            severity="info",
                        )
                    )

        return issues


def detect_language(src_path: str) -> Optional[Language]:
    """Auto-detect programming language from file extensions"""
    extensions: Dict[str, int] = {}

    for _root, _dirs, files in os.walk(src_path):
        for file in files:
            ext = Path(file).suffix.lower()
            extensions[ext] = extensions.get(ext, 0) + 1

    # Determine primary language
    if extensions.get(".py", 0) > 0:
        return Language.PYTHON
    elif extensions.get(".java", 0) > 0:
        return Language.JAVA
    elif extensions.get(".ts", 0) > extensions.get(".js", 0):
        return Language.TYPESCRIPT
    elif extensions.get(".js", 0) > 0:
        return Language.JAVASCRIPT
    elif extensions.get(".go", 0) > 0:
        return Language.GO
    elif extensions.get(".rs", 0) > 0:
        return Language.RUST
    elif extensions.get(".cs", 0) > 0:
        return Language.CSHARP

    return None


def get_source_files(src_path: str, language: Language) -> List[str]:
    """Get list of source files for the given language"""
    extensions_map = {
        Language.PYTHON: [".py"],
        Language.JAVA: [".java"],
        Language.JAVASCRIPT: [".js"],
        Language.TYPESCRIPT: [".ts", ".tsx"],
        Language.GO: [".go"],
        Language.RUST: [".rs"],
        Language.CSHARP: [".cs"],
    }

    extensions = extensions_map.get(language, [])
    source_files = []

    for root, dirs, files in os.walk(src_path):
        # Skip common non-source directories
        dirs[:] = [
            d
            for d in dirs
            if d not in ["node_modules", "__pycache__", ".git", "target", "build"]
        ]

        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                source_files.append(os.path.join(root, file))

    return source_files


def print_report(issues: List[PatternIssue], verbose: bool = False):
    """Print validation report"""
    if not issues:
        print("✅ No design pattern issues found!")
        return

    # Group issues by severity
    errors = [i for i in issues if i.severity == "error"]
    warnings = [i for i in issues if i.severity == "warning"]
    infos = [i for i in issues if i.severity == "info"]

    print("\n🏗️ Design Pattern Validation Report")
    print(f"{'='*50}")
    print(
        f"Total Issues: {len(issues)} (Errors: {len(errors)}, Warnings: {len(warnings)}, Info: {len(infos)})"
    )

    for severity, issue_list in [
        ("ERROR", errors),
        ("WARNING", warnings),
        ("INFO", infos),
    ]:
        if not issue_list:
            continue

        print(f"\n{severity}S ({len(issue_list)}):")
        print("-" * 20)

        for issue in issue_list:
            print(f"📁 {issue.file_path}:{issue.line_number}")
            print(f"🏗️  Pattern: {issue.pattern.value.title()}")
            print(f"⚠️  Issue: {issue.issue_type}")
            print(f"📝 {issue.description}")
            if verbose:
                print(f"🔍 Severity: {issue.severity}")
            print()


def main():
    parser = argparse.ArgumentParser(description="Universal Design Pattern Validator")
    parser.add_argument("src_path", help="Path to source code directory")
    parser.add_argument(
        "--language",
        choices=[lang.value for lang in Language],
        help="Programming language (auto-detected if not specified)",
    )
    parser.add_argument(
        "--patterns", help="Comma-separated list of patterns to check (default: all)"
    )
    parser.add_argument(
        "--auto-detect",
        action="store_true",
        help="Auto-detect language from file extensions",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit with non-zero code if issues found",
    )

    args = parser.parse_args()

    if not os.path.exists(args.src_path):
        print(f"❌ Error: Source path '{args.src_path}' does not exist")
        sys.exit(1)

    # Determine language
    if args.language:
        language = Language(args.language)
    elif args.auto_detect:
        language = detect_language(args.src_path)
        if not language:
            print("❌ Error: Could not auto-detect language")
            sys.exit(1)
        print(f"🔍 Auto-detected language: {language.value}")
    else:
        print("❌ Error: Please specify --language or use --auto-detect")
        sys.exit(1)

    # Get source files
    source_files = get_source_files(args.src_path, language)
    if not source_files:
        print(f"❌ No source files found for language: {language.value}")
        sys.exit(1)

    print(f"🔍 Analyzing {len(source_files)} {language.value} files...")

    # Validate patterns
    validator = PatternValidator(language)
    all_issues = []

    for file_path in source_files:
        try:
            file_issues = validator.validate_file(file_path)
            all_issues.extend(file_issues)
        except Exception as e:
            if args.verbose:
                print(f"⚠️  Error analyzing {file_path}: {e}")

    # Print report
    print_report(all_issues, args.verbose)

    # Exit with appropriate code
    if args.exit_code and all_issues:
        error_count = len([i for i in all_issues if i.severity == "error"])
        sys.exit(1 if error_count > 0 else 0)


if __name__ == "__main__":
    main()
