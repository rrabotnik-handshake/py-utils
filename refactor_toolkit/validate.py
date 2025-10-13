#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Refactor Validation Tool

A simple, focused tool to validate code safety after refactoring.
Answers one question: "Is my code safe to deploy after this change?"

Usage:
    python validate.py                    # Standard validation (default)
    python validate.py --mode quick       # 5–10 minute essential checks
    python validate.py --tech javascript  # Override default Python detection
    python validate.py --output report.md # Save report to file
    python validate.py --strict           # Exit non-zero on NEEDS ATTENTION
    python validate.py --patterns         # Include design pattern validation
"""

import argparse
import json
import shutil
import subprocess  # nosec B404: subprocess is used safely for validation commands
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class TechStack(Enum):
    PYTHON = "python"
    # Future language support (commented out for now)
    # JAVASCRIPT = "javascript"
    # JAVA = "java"
    # GO = "go"
    # RUST = "rust"
    # GENERIC = "generic"


class ValidationLayer(Enum):
    CODE_QUALITY = "Code Quality"
    MAINTAINABILITY = "Maintainability"
    UNIT_TESTS = "Unit Tests"
    INTEGRATION = "Integration"
    PATTERNS = "Design Patterns"
    PERFORMANCE = "Performance"
    SECURITY = "Security"


# Note: Time estimates removed - actual duration shown in summary


@dataclass
class ValidationResult:
    name: str
    passed: bool
    message: str
    duration: float
    required: bool = True
    layer: Optional[ValidationLayer] = None
    remediation_tip: Optional[str] = None
    full_output: Optional[str] = None
    command: Optional[str] = None


@dataclass
class LayerSummary:
    layer: ValidationLayer
    checks_passed: int
    total_checks: int
    score_percent: int
    status: str


@dataclass
class ProductionReadiness:
    functionality: str = "Unknown"
    reliability: str = "Unknown"
    performance: str = "Unknown"
    maintainability: str = "Unknown"
    security: str = "Unknown"
    evidence: Dict[str, str] = field(default_factory=dict)


@dataclass
class ValidationSummary:
    tech_stack: TechStack
    total_checks: int
    passed_checks: int
    failed_checks: int
    score_percent: int
    duration: float
    results: List[ValidationResult]
    # Enhanced fields for comprehensive reporting
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    project_name: str = ""
    layer_summaries: List[LayerSummary] = field(default_factory=list)
    production_readiness: ProductionReadiness = field(
        default_factory=ProductionReadiness
    )
    confidence: str = "Medium"
    risk_level: str = "Medium"
    final_recommendation: Dict[str, Any] = field(default_factory=dict)


class RefactorValidator:
    """Simple, focused refactor validation tool."""

    def __init__(self, project_dir: str = ".", verbose: bool = False):
        self.project_dir = Path(project_dir).resolve()
        self.start_time = time.time()
        self.verbose = verbose
        self.python_cmd = self._detect_python_command()
        self.pip_cmd = self._detect_pip_command()

    def _detect_python_command(self) -> str:
        """Detect the correct Python command to use."""
        import shutil

        for cmd in ["python", "python3", "py"]:
            if shutil.which(cmd):
                return cmd
        return "python"  # fallback

    def _detect_pip_command(self) -> str:
        """Detect the correct pip command to use."""
        # Always prefer python -m pip for consistency
        return f"{self.python_cmd} -m pip"

    def _has_trunk(self) -> bool:
        """Check if Trunk is available and configured."""
        import shutil

        return (
            shutil.which("trunk") is not None
            and (self.project_dir / ".trunk" / "trunk.yaml").exists()
        )

    def _substitute_commands(self, command: str) -> str:
        """Replace hardcoded commands with detected ones for terminal-agnostic execution."""
        # Replace python module invocations
        for py_cmd in ("python -m", "python3 -m", "py -m"):
            command = command.replace(py_cmd, f"{self.python_cmd} -m")
        # Replace pip unless already using any python -m pip variant
        if " -m pip" not in command:
            command = command.replace(" pip ", f" {self.pip_cmd} ")
            command = command.replace(" pip3 ", f" {self.pip_cmd} ")
        if command.startswith("pip "):
            command = command.replace("pip ", f"{self.pip_cmd} ", 1)
        if command.startswith("pip3 "):
            command = command.replace("pip3 ", f"{self.pip_cmd} ", 1)
        return command

    # Note: detect_tech_stack() removed - tech stack is now explicitly passed via --tech CLI argument
    # Future enhancement: could auto-detect when --tech not provided by checking for:
    # - package.json → JavaScript
    # - pom.xml/build.gradle → Java
    # - go.mod → Go
    # - Cargo.toml → Rust

    def run_check(
        self,
        name: str,
        command: str,
        success_msg: str,
        fail_msg: str,
        required: bool = True,
        layer: Optional[ValidationLayer] = None,
        remediation_tip: Optional[str] = None,
    ) -> ValidationResult:
        """Run a single validation check."""
        start = time.time()

        # Make command terminal agnostic
        command = self._substitute_commands(command)

        # Show what we're about to run (verbose mode only)
        if self.verbose:
            print(f"🔍 Running: {name}")
            print(f"   Command: {command}")
        else:
            # Minimal output - just show we're running the check
            print(f"🔍 {name}...", end=" ", flush=True)

        try:
            # Run command and capture output
            result = subprocess.run(
                command,
                shell=True,  # nosec B602: Commands are internally constructed validation tools, not user input
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            duration = time.time() - start
            passed = result.returncode == 0
            status_icon = "✅" if passed else "❌"
            message = success_msg if passed else fail_msg

            if self.verbose:
                required_mark = " (REQUIRED)" if required else " (optional)"
                print(
                    f"   Result: {status_icon} {name}{required_mark} - {duration:.1f}s"
                )
                if passed:
                    print(f"   ✅ {success_msg}")
                else:
                    error_preview = (result.stderr or result.stdout or "").strip()[:200]
                    if error_preview:
                        print(f"   ❌ {fail_msg}\n   Preview: {error_preview}")
                    else:
                        print(f"   ❌ {fail_msg}")
                print()
            else:
                if passed:
                    print(f"{status_icon} ({duration:.1f}s)")
                else:
                    print(f"{status_icon} FAILED ({duration:.1f}s)")
                    # show a short preview on failure in minimal mode
                    preview = (result.stderr or result.stdout or "").strip()[:120]
                    if preview:
                        print(f"   💬 {preview}")

            full_output = (
                None
                if passed
                else f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
            return ValidationResult(
                name=name,
                passed=passed,
                message=message,
                duration=duration,
                required=required,
                layer=layer,
                remediation_tip=remediation_tip,
                full_output=full_output,
                command=command,
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start
            if self.verbose:
                print(f"   ⏰ TIMEOUT after {duration:.1f}s")
                print(f"   ❌ {fail_msg}: Command timed out\n")
            else:
                print(f"❌ TIMEOUT ({duration:.1f}s)")
            return ValidationResult(
                name=name,
                passed=False,
                message=f"{fail_msg}: Command timed out",
                duration=duration,
                required=required,
                layer=layer,
                remediation_tip=remediation_tip,
                full_output=None,
                command=command,
            )
        except Exception as e:
            duration = time.time() - start
            if self.verbose:
                print(f"   💥 ERROR after {duration:.1f}s")
                print(f"   ❌ {fail_msg}: {str(e)}\n")
            else:
                print(f"❌ ERROR ({duration:.1f}s)")
            return ValidationResult(
                name=name,
                passed=False,
                message=f"{fail_msg}: {str(e)}",
                duration=duration,
                required=required,
                layer=layer,
                remediation_tip=remediation_tip,
                full_output=None,
                command=command,
            )

    def validate_python(
        self, since: Optional[str] = None, category_filter: str = "all"
    ) -> List[ValidationResult]:
        """Python-specific validation checks (comprehensive mode)."""
        results = []

        # Get target files for optimization
        targets = self._since_glob(since)

        # Helper to conditionally run and add check based on category filter
        def run_and_add_if_match(check_name, *args, **kwargs):
            """Only run and add check if it matches the category filter."""
            if category_filter == "all" or self.should_run_check(
                check_name, category_filter
            ):
                results.append(self.run_check(check_name, *args, **kwargs))

        # Python Syntax check
        run_and_add_if_match(
            "Python Syntax",
            f"{self.python_cmd} -m compileall -q {targets}",
            "All Python files compile successfully",
            "Python syntax errors found",
            required=True,
            layer=ValidationLayer.CODE_QUALITY,
            remediation_tip="Fix syntax errors shown above, then re-run validation",
        )

        # Code Quality check
        if self._has_trunk():
            code_quality_cmd = "trunk check --filter=ruff"
            remediation = "Run `trunk fmt` to auto-fix issues, or `trunk check --filter=ruff` for details"
        else:
            excludes = " --exclude */lib/python* --exclude *venv* --exclude __pycache__ --exclude node_modules --exclude build --exclude dist --exclude .git"
            code_quality_cmd = (
                f"{self.python_cmd} -m ruff check {targets}{excludes} "
                f"|| (echo 'Installing ruff...' && {self.pip_cmd} install ruff && {self.python_cmd} -m ruff check {targets}{excludes}) "
                f"|| (echo 'Installing flake8...' && {self.pip_cmd} install flake8 && {self.python_cmd} -m flake8 {targets} --count --statistics)"
            )
            remediation = (
                "Run `ruff --fix` where safe; otherwise fix highest-severity first"
            )

        run_and_add_if_match(
            "Code Quality",
            code_quality_cmd,
            "Code passes linting standards",
            "Code quality issues detected",
            required=True,
            layer=ValidationLayer.CODE_QUALITY,
            remediation_tip=remediation,
        )

        run_and_add_if_match(
            "Import Dependencies",
            f"""{self.python_cmd} -c "import importlib, pathlib, sys, os
# Add src directory to Python path for local development
cwd = os.getcwd()
src_path = os.path.join(cwd, 'src') if not cwd.endswith('/src') else cwd
if os.path.exists(src_path) and src_path not in sys.path:
    sys.path.insert(0, src_path)
# Skip import check if we're inside a package directory (like src/)
if any(marker in cwd for marker in ['/src', '/tests', '/schema_diff']):
    print('Skipping import check for package directory'); sys.exit(0)
# Only check files that should be importable as standalone modules
exclude_dirs = {{'.git', '__pycache__', 'venv', '.venv', 'build', 'dist', 'site-packages', 'lib', 'coresignal', 'tests'}}
exclude_patterns = ['src/', 'test_', 'conftest.py']
files=[p for p in pathlib.Path('.').rglob('*.py') 
       if not any(s in str(p) for s in exclude_dirs) 
       and not any(str(p).startswith(pattern) for pattern in exclude_patterns)
       and p.stem not in ('__init__','setup')
       and not str(p).startswith('./coresignal/')
       and not str(p).startswith('./src/')]
ok=True
for f in files:
    try:
        importlib.import_module(f.stem)
    except Exception as e:
        print(f'Import error in {{f}}: {{e}}'); ok=False
sys.exit(0 if ok else 1)" """,
            "Core modules import successfully",
            "Import dependency issues found",
            required=True,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Check import paths and ensure all dependencies are installed",
        )

        run_and_add_if_match(
            "Type Checking",
            f"{self.python_cmd} -m mypy {self._mypy_target()} --show-error-codes || (echo 'Installing mypy...' && {self.pip_cmd} install mypy && {self.python_cmd} -m mypy {self._mypy_target()} --show-error-codes)",
            "Static type analysis passed",
            "Type checking errors detected",
            required=True,
            layer=ValidationLayer.CODE_QUALITY,
            remediation_tip="Fix type errors shown above; add type hints where missing",
        )

        run_and_add_if_match(
            "Security Scan",
            f"{self.python_cmd} -m bandit -r src/ tests/ refactor_toolkit/ -f txt --skip B101,B311 || (echo 'Installing bandit...' && {self.pip_cmd} install bandit && {self.python_cmd} -m bandit -r src/ tests/ refactor_toolkit/ -f txt --skip B101,B311)",
            "No security vulnerabilities detected",
            "Security issues found",
            required=True,
            layer=ValidationLayer.SECURITY,
            remediation_tip="Review and fix security issues; consider using safer alternatives",
        )

        run_and_add_if_match(
            "Pre-commit Hooks",
            f"""{self.python_cmd} -c "import os,sys,subprocess
if not os.path.isdir('.git'):
    print('Not a git repo; skipping pre-commit'); sys.exit(0)
try:
    r=subprocess.run(['pre-commit','run','--all-files','--show-diff-on-failure'])
    sys.exit(r.returncode)
except FileNotFoundError:
    pass
r=subprocess.run([{self.python_cmd!r},'-m','pip','install','pre-commit']).returncode
if r!=0: sys.exit(1)
subprocess.run(['pre-commit','install'])
sys.exit(subprocess.run(['pre-commit','run','--all-files','--show-diff-on-failure']).returncode)" """,
            "All pre-commit hooks passed",
            "Pre-commit hook failures detected",
            required=True,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Fix issues shown by pre-commit hooks; run `pre-commit run --all-files` locally",
        )

        run_and_add_if_match(
            "Unit Tests",
            f"{self.python_cmd} -m pytest tests/ --tb=short -v --durations=5 || (echo 'Installing pytest...' && {self.pip_cmd} install pytest && {self.python_cmd} -m pytest tests/ --tb=short -v --durations=5) || {self.python_cmd} -m unittest discover -s tests -p 'test_*.py' -v",
            "All tests passed successfully",
            "Test failures or errors detected",
            required=True,
            layer=ValidationLayer.UNIT_TESTS,
            remediation_tip="Run `pytest -vv` and fix top failures first; check test dependencies",
        )

        # Coverage gate (opportunistic)
        run_and_add_if_match(
            "Coverage Threshold",
            f"""{self.python_cmd} -c "import os,sys,xml.etree.ElementTree as ET
p='coverage.xml'
if not os.path.exists(p):
    print('No coverage.xml found - skipping coverage check'); sys.exit(0)
try:
    rate=float(ET.parse(p).getroot().attrib.get('line-rate',0))
    print(f'Coverage: {{rate:.1%}}')
    sys.exit(0 if rate>=0.8 else 1)
except Exception as e:
    print(f'Coverage file exists but could not parse: {{e}}'); sys.exit(0)" """,
            "Coverage meets threshold (≥80%)",
            "Coverage below 80% threshold",
            required=False,
            layer=ValidationLayer.UNIT_TESTS,
            remediation_tip="Add tests for uncovered code paths; run `pytest --cov` to generate coverage report",
        )

        run_and_add_if_match(
            "Package Dependencies",
            f"{self.python_cmd} -m pip check",
            "All dependencies compatible",
            "Package dependency conflicts found",
            required=True,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Run `pip check` and resolve dependency conflicts; update requirements",
        )

        # Vulnerability scanning
        run_and_add_if_match(
            "Vulnerability Scan",
            f"{self.python_cmd} -m pip_audit --desc || (echo 'Installing pip-audit...' && {self.pip_cmd} install pip-audit && {self.python_cmd} -m pip_audit --desc)",
            "No known vulnerabilities detected",
            "Security vulnerabilities found in dependencies",
            required=False,
            layer=ValidationLayer.SECURITY,
            remediation_tip="Upgrade vulnerable packages; add temporary ignores with expiry dates if needed",
        )

        # Complexity and maintainability checks
        run_and_add_if_match(
            "Code Complexity",
            "radon cc . -a -nb --min C --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git' || (echo 'Installing radon...' && pip install radon && radon cc . -a -nb --min C --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git')",
            "Code complexity acceptable",
            "High complexity detected",
            required=False,
            layer=ValidationLayer.MAINTAINABILITY,
            remediation_tip="Refactor D/E/F rated functions; split complex methods into smaller ones",
        )

        run_and_add_if_match(
            "Maintainability Index",
            "radon mi . -s -n B --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git' || (echo 'Installing radon...' && pip install radon && radon mi . -s -n B --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git')",
            "Code maintainability acceptable",
            "Low maintainability detected",
            required=False,
            layer=ValidationLayer.MAINTAINABILITY,
            remediation_tip="Improve code structure, reduce complexity, and enhance readability",
        )

        # Dead code detection
        run_and_add_if_match(
            "Dead Code Analysis",
            "vulture . --min-confidence 60 --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git' || (echo 'Installing vulture...' && pip install vulture && vulture . --min-confidence 60 --exclude='*/lib/python*,*venv*,__pycache__,node_modules,build,dist,.git')",
            "No obvious dead code detected",
            "Dead code candidates found",
            required=False,
            layer=ValidationLayer.MAINTAINABILITY,
            remediation_tip="Review and remove unused code; verify false positives before deletion",
        )

        # Secret scanning (Unix tools required)
        if self._has_unix_tools():
            run_and_add_if_match(
                "Secret Scan",
                r"""git ls-files -z | xargs -0 grep -nEI --max-count=1 '(AWS_ACCESS_KEY_ID|AKIA[0-9A-Z]{16}|BEGIN RSA PRIVATE KEY|ghp_[A-Za-z0-9]{36}|sk-[A-Za-z0-9]{48}|xox[baprs]-[A-Za-z0-9-]{10,})' || true""",
                "No secrets detected in repository",
                "Potential secrets or credentials found",
                required=True,
                layer=ValidationLayer.SECURITY,
                remediation_tip="Rotate exposed credentials immediately; remove from git history with git filter-repo",
            )
        else:
            results.append(
                ValidationResult(
                    "Secret Scan",
                    True,
                    "Unix tools not available - skipping secret scan",
                    0.0,
                    required=False,
                    layer=ValidationLayer.SECURITY,
                    remediation_tip="Install grep/xargs for secret scanning, or use a dedicated tool like truffleHog",
                    full_output=None,
                    command=None,
                )
            )

        # Git hygiene checks (informational only, verbose mode)
        if self.verbose:
            run_and_add_if_match(
                "Working Tree Status",
                'test -z "$(git status --porcelain)"',
                "Working tree is clean",
                "Uncommitted changes detected (informational)",
                required=False,
                layer=ValidationLayer.INTEGRATION,
                remediation_tip="This is informational - uncommitted changes during development are normal",
            )

            # Large binary check (Unix tools required)
        if self._has_unix_tools():
            run_and_add_if_match(
                "Large Binary Check",
                'git ls-files -s | awk \'{if($4!="") print $4}\' | xargs -I{} sh -c \'test -f "{}" && du -k "{}"\' 2>/dev/null | awk \'$1>5120{print $2 " (" $1 "KB)"; exit 1}\' || { echo \'No large binaries found\'; exit 0; }',
                "No large binaries in repository",
                "Large binary files detected (>5MB)",
                required=False,
                layer=ValidationLayer.PERFORMANCE,
                remediation_tip="Use Git LFS for large files or store artifacts externally; consider .gitignore updates",
            )
        else:
            results.append(
                ValidationResult(
                    "Large Binary Check",
                    True,
                    "Unix tools not available - skipping binary size check",
                    0.0,
                    required=False,
                    layer=ValidationLayer.PERFORMANCE,
                    remediation_tip="Install awk/xargs for binary size checks, or manually review large files",
                    full_output=None,
                    command=None,
                )
            )

        # Repository metadata checks
        run_and_add_if_match(
            "Repository Metadata",
            f"""{self.python_cmd} -c "import os,sys; sys.exit(0 if os.path.isfile('README.md') else 1)" """,
            "README file present",
            "Missing README.md (informational)",
            required=False,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Add README.md with project description (optional, improves documentation)",
        )

        run_and_add_if_match(
            "Project Documentation",
            f"""{self.python_cmd} -c "import os,sys; sys.exit(0 if (os.path.isfile('CONTRIBUTING.md') or os.path.isfile('.github/CONTRIBUTING.md') or os.path.isfile('docs/CONTRIBUTING.md')) else 0)" """,
            "Contributing guidelines present",
            "No contributing guidelines (informational)",
            required=False,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Add CONTRIBUTING.md to help new contributors (optional)",
        )

        # Performance smoke tests (language-agnostic)
        # 21) Hot file hotspots - detect oversized files (Unix tools required)
        if self._has_unix_tools():
            run_and_add_if_match(
                "Giant Files",
                r"""git ls-files '*.py' '*.ts' | xargs wc -l 2>/dev/null | awk '$1>800 && $2!="total"{print $2 " (" $1 " lines)"; found=1} END{exit found?1:0}' || { echo 'No oversized files detected'; exit 0; }""",
                "No oversized source files detected",
                "Oversized files found - consider refactoring",
                required=False,
                layer=ValidationLayer.PERFORMANCE,
                remediation_tip="Break large files (>800 LOC) into smaller, focused modules for better maintainability",
            )
        else:
            results.append(
                ValidationResult(
                    "Giant Files",
                    True,
                    "Unix tools not available - skipping file size check",
                    0.0,
                    required=False,
                    layer=ValidationLayer.PERFORMANCE,
                    remediation_tip="Install awk/xargs for file size checks, or manually review large source files",
                    full_output=None,
                    command=None,
                )
            )

        # 22) N+1 or heavy I/O risk markers - detect potential performance issues (Unix tools required)
        if self._has_unix_tools():
            run_and_add_if_match(
                "I/O Risk Patterns",
                r"""find . -name "*.py" -not -path "./.git/*" -not -path "./venv/*" -not -path "./.venv/*" -not -path "./*venv/*" -not -path "./*/lib/python*" -not -path "./__pycache__/*" -not -path "./node_modules/*" -not -path "./build/*" -not -path "./dist/*" -exec grep -l -E "(requests\.|urllib\.|\.query\(|\.execute\(|\.fetchall\()" {} \; | xargs -I {} grep -n -E "(for |while )" {} | head -5 | grep -E "(requests\.|urllib\.|\.query\(|\.execute\(|\.fetchall\()" && { echo '--- Potential I/O in loops detected ---'; exit 1; } || { echo 'No obvious I/O-in-loop patterns detected'; exit 0; }""",
                "No obvious I/O-in-loop patterns detected",
                "Potential N+1 or heavy I/O patterns found",
                required=False,
                layer=ValidationLayer.PERFORMANCE,
                remediation_tip="Review flagged patterns: consider batching requests, caching, or moving I/O outside loops",
            )
        else:
            results.append(
                ValidationResult(
                    "I/O Risk Patterns",
                    True,
                    "Unix tools not available - skipping I/O pattern check",
                    0.0,
                    required=False,
                    layer=ValidationLayer.PERFORMANCE,
                    remediation_tip="Install find/grep for I/O pattern checks, or manually review for N+1 query patterns",
                    full_output=None,
                    command=None,
                )
            )

        # 23) Environment parity - CI awareness
        run_and_add_if_match(
            "Tool Versions",
            f"""{self.python_cmd} -c "import sys; print(f'Python: {{sys.version}}'); import platform; print(f'Platform: {{platform.platform()}}'); exec('try:\\n import pytest; print(f\\"pytest: {{pytest.__version__}}\\")\\nexcept: print(\\"pytest: not installed\\")'); exec('try:\\n import mypy; print(f\\"mypy: {{mypy.__version__}}\\")\\nexcept: print(\\"mypy: not installed\\")'); exec('try:\\n import ruff; print(f\\"ruff: {{ruff.__version__}}\\")\\nexcept: print(\\"ruff: not installed\\")')" 2>/dev/null || {{ echo "Could not capture all tool versions"; exit 0; }}""",
            "Captured tool versions for reproducibility",
            "Could not capture complete tool versions",
            required=False,
            layer=ValidationLayer.INTEGRATION,
            remediation_tip="Ensure consistent tool versions across environments using requirements.txt or pyproject.toml",
        )

        return results

    # Future language support - commented out for now
    # def validate_javascript(self, mode: ValidationMode, since: Optional[str] = None) -> List[ValidationResult]:
    #     """JavaScript/Node.js validation checks."""
    #     results = []
    #     # Implementation commented out - future enhancement
    #     return results

    # def validate_java(self, mode: ValidationMode, since: Optional[str] = None) -> List[ValidationResult]:
    #     """Java validation checks."""
    #     results = []
    #     # Implementation commented out - future enhancement
    #     return results

    # def validate_go(self, mode: ValidationMode, since: Optional[str] = None) -> List[ValidationResult]:
    #     """Go validation checks."""
    #     results = []
    #     # Implementation commented out - future enhancement
    #     return results

    # def validate_rust(self, mode: ValidationMode, since: Optional[str] = None) -> List[ValidationResult]:
    #     """Rust validation checks."""
    #     results = []
    #     # Implementation commented out - future enhancement
    #     return results

    # def validate_generic(self, mode: ValidationMode, since: Optional[str] = None) -> List[ValidationResult]:
    #     """Generic validation for unknown tech stacks."""
    #     results = []
    #     # Implementation commented out - future enhancement
    #     return results

    def check_tool_availability(self, tool_name: str) -> bool:
        """Check if a tool is available in PATH."""
        return shutil.which(tool_name) is not None

    def _has_unix_tools(self) -> bool:
        """Check if Unix tools (grep, awk, find) are available."""
        return all(shutil.which(tool) for tool in ["grep", "awk", "find"])

    def _mypy_target(self) -> str:
        """Detect a sensible target directory for mypy."""
        candidates = ["src"]
        # Add top-level package folders
        for p in self.project_dir.iterdir():
            if p.is_dir() and (p / "__init__.py").exists():
                candidates.append(p.name)
        for c in candidates:
            if (self.project_dir / c).exists():
                return c
        return "."

    def validate_patterns(self) -> ValidationResult:
        """Run design pattern validation if validate_patterns.py exists."""
        patterns_script = self.project_dir / "validate_patterns.py"
        if not patterns_script.exists():
            return ValidationResult(
                "Design Patterns",
                True,
                "Pattern validation script not found (optional)",
                0.0,
                required=False,
                layer=ValidationLayer.PATTERNS,
                remediation_tip="Create validate_patterns.py for design pattern checks",
                full_output=None,
                command=None,
            )

        # Enhanced design pattern analysis with JSON output
        cmd = f"{self.python_cmd} {patterns_script} . --json"
        if (self.project_dir / "pattern_config.json").exists():
            cmd += f" --config {self.project_dir}/pattern_config.json"

        return self.run_check(
            "Design Patterns",
            cmd,
            "No pattern issues found",
            "Design pattern issues detected",
            required=False,
            layer=ValidationLayer.PATTERNS,
            remediation_tip="Fix cycles first, then high fan-out, then LoD/Feature Envy violations",
        )

    def calculate_layer_summaries(
        self, results: List[ValidationResult]
    ) -> List[LayerSummary]:
        """Calculate per-layer validation summaries."""
        layer_results = {}

        # Group results by layer
        for result in results:
            layer = result.layer or ValidationLayer.CODE_QUALITY  # Default layer
            if layer not in layer_results:
                layer_results[layer] = []
            layer_results[layer].append(result)

        summaries = []
        for layer, layer_results_list in layer_results.items():
            passed = sum(1 for r in layer_results_list if r.passed)
            total = len(layer_results_list)
            score = int((passed / total) * 100) if total > 0 else 100

            if score >= 90:
                status = "EXCELLENT"
            elif score >= 70:
                status = "GOOD"
            elif score >= 50:
                status = "NEEDS ATTENTION"
            else:
                status = "CRITICAL"

            summaries.append(
                LayerSummary(
                    layer=layer,
                    checks_passed=passed,
                    total_checks=total,
                    score_percent=score,
                    status=status,
                )
            )

        return summaries

    def assess_production_readiness(
        self, results: List[ValidationResult], score_percent: int
    ) -> ProductionReadiness:
        """Assess production readiness across different dimensions."""
        readiness = ProductionReadiness()

        # Group results by layer
        by_layer = {}
        for r in results:
            layer = r.layer or ValidationLayer.CODE_QUALITY
            by_layer.setdefault(layer, []).append(r)

        def layer_ok(layer):
            xs = by_layer.get(layer, [])
            return xs and all(x.passed or not x.required for x in xs)

        readiness.functionality = (
            "READY" if layer_ok(ValidationLayer.UNIT_TESTS) else "NEEDS WORK"
        )
        readiness.reliability = "READY" if score_percent >= 80 else "NEEDS WORK"
        readiness.performance = (
            "READY"
            if layer_ok(ValidationLayer.PERFORMANCE) or score_percent >= 70
            else "NEEDS WORK"
        )
        readiness.maintainability = (
            "READY" if layer_ok(ValidationLayer.CODE_QUALITY) else "NEEDS WORK"
        )
        readiness.security = (
            "READY" if layer_ok(ValidationLayer.SECURITY) else "NEEDS WORK"
        )

        readiness.evidence = {
            layer.value: ("PASSED" if layer_ok(layer) else "FAILED")
            for layer in ValidationLayer
        }
        readiness.evidence["overall_score"] = f"{score_percent}%"

        return readiness

    def extract_actionable_errors(self, results: List[ValidationResult]) -> List[str]:
        """Extract specific actionable errors from failed validation results."""
        actionable_errors = []
        optional_errors = []
        seen_errors = set()  # Global deduplication across all results

        for result in results:
            if not result.passed:
                error_details = self._parse_specific_errors(result)
                if error_details:
                    # Add only unique errors
                    for error in error_details:
                        if error not in seen_errors:
                            if result.required:
                                actionable_errors.append(error)
                            else:
                                optional_errors.append(f"{error} (optional)")
                            seen_errors.add(error)
                else:
                    # Fallback to generic error with remediation tip
                    error_msg = f"**{result.name}**: {result.message}"
                    if result.remediation_tip:
                        error_msg += f" → {result.remediation_tip}"
                    if error_msg not in seen_errors:
                        if result.required:
                            actionable_errors.append(error_msg)
                        else:
                            optional_errors.append(f"{error_msg} (optional)")
                        seen_errors.add(error_msg)

        # Return required errors first, then optional errors
        return actionable_errors + optional_errors

    def extract_categorized_errors(self, results: List[ValidationResult]) -> dict:
        """Extract errors grouped by category for better readability."""
        categories = {
            "Code Quality": {"errors": [], "commands": set()},
            "Security": {"errors": [], "commands": set()},
            "Dependencies": {"errors": [], "commands": set()},
            "Tests": {"errors": [], "commands": set()},
            "Dead Code": {"errors": [], "commands": set()},
            "Documentation": {"errors": [], "commands": set()},
            "Other": {"errors": [], "commands": set()},
        }

        seen_errors = set()

        for result in results:
            if not result.passed:
                error_details = self._parse_specific_errors(result)
                errors = error_details if error_details else [f"{result.message}"]

                # Determine category and get run command
                category = self._categorize_result(result)
                run_command = self._get_run_command(result)

                # Add errors to appropriate category
                for error in errors:
                    if error and error not in seen_errors:
                        categories[category]["errors"].append(error)
                        if run_command:
                            categories[category]["commands"].add(run_command)
                        seen_errors.add(error)

        # Remove empty categories and convert to simpler format
        result_dict = {}
        for category, data in categories.items():
            if data["errors"]:
                result_dict[category] = {
                    "errors": data["errors"],
                    "commands": list(data["commands"]),
                }

        return result_dict

    def _categorize_result(self, result: ValidationResult) -> str:
        """Determine which category a result belongs to."""
        name = result.name
        message = result.message

        if "Code Quality" in name or "Pre-commit" in name or "Hook" in name:
            return "Code Quality"
        elif "Test" in name:
            return "Tests"
        elif (
            "Security" in name or "Vulnerability" in name or "Vulnerability" in message
        ):
            return "Security"
        elif "Import" in name or "Dependencies" in name or "Package" in name:
            return "Dependencies"
        elif "Dead Code" in name:
            return "Dead Code"
        elif "Documentation" in name or "README" in message:
            return "Documentation"
        else:
            return "Other"

    def _get_run_command(self, result: ValidationResult) -> str:
        """Get the command to run for more details about a specific check."""
        name = result.name

        # Map check names to their run commands
        command_map = {
            "Code Quality": "trunk check --all",
            "Type Checking": "mypy src/ --show-error-codes",
            "Security Scan": "bandit -r . --exclude ./venv,./coresignal",
            "Pre-commit Hooks": "pre-commit run --all-files",
            "Unit Tests": "pytest -vv",
            "Vulnerability Scan": "pip-audit",
            "Dead Code Analysis": "vulture . --min-confidence 60",
            "Design Patterns": "python validate_patterns.py",
        }

        return command_map.get(name, "")

    def should_run_check(self, check_name: str, category_filter: str) -> bool:
        """Determine if a check should run based on category filter."""
        if category_filter == "all":
            return True

        # Map category filter to check names (comprehensive list based on ValidationLayer)
        category_map = {
            "code-quality": [
                "Python Syntax",
                "Code Quality",
                "Pre-commit Hooks",
                "Type Checking",
                "Code Complexity",
                "Maintainability Index",
            ],
            "security": ["Vulnerability Scan", "Secret Scan"],
            "tests": ["Unit Tests", "Coverage Threshold"],
            "dependencies": [
                "Import Dependencies",
                "Package Dependencies",
                "Import Smoke",
            ],
            "dead-code": ["Dead Code Analysis"],
            "patterns": ["Design Patterns"],
            "documentation": ["Repository Metadata", "Project Documentation"],
            "other": [
                "I/O Risk Patterns",
                "Large Binary Check",
                "Giant Files",
                "Tool Versions",
                "Working Tree Status",
            ],
        }

        checks_for_category = category_map.get(category_filter, [])
        return check_name in checks_for_category

    def _truncate_long_text(self, text: str, max_length: int = 200) -> str:
        """Truncate long text and add ellipsis if needed."""
        if len(text) <= max_length:
            return text

        # Try to truncate at a sentence boundary
        truncated = text[:max_length]
        last_period = truncated.rfind(".")
        last_space = truncated.rfind(" ")

        if last_period > max_length * 0.7:  # If period is reasonably close to end
            return truncated[: last_period + 1] + " [...see --verbose for full details]"
        elif last_space > max_length * 0.8:  # If space is close to end
            return truncated[:last_space] + "... [see --verbose for full details]"
        else:
            return truncated + "... [see --verbose for full details]"

    def _parse_specific_errors(self, result: ValidationResult) -> List[str]:
        """Parse specific errors from validation result output."""
        if not result.full_output:
            return []

        errors = []
        output = result.full_output

        # Check for timeout first
        if "Command timed out" in result.message or "TIMEOUT" in result.message:
            errors.append(
                f"**Timeout**: {result.name} exceeded time limit - consider optimizing or increasing timeout"
            )
            return errors

        # Check for command not found errors first (terminal agnostic)
        if (
            "command not found" in output.lower()
            or "not recognized as an internal" in output.lower()
        ):
            errors.extend(self._parse_command_not_found_errors(output, result.name))
            return errors  # Return early for command not found issues

        # Parse different types of errors based on the check name
        if "Code Quality" in result.name:
            if "trunk check" in result.command:
                errors.extend(self._parse_trunk_errors(output))
            else:
                errors.extend(self._parse_ruff_errors(output))
        elif "Type Checking" in result.name:
            errors.extend(self._parse_mypy_errors(output))
        elif "Security Scan" in result.name:
            errors.extend(self._parse_bandit_errors(output))
        elif "Pre-commit Hooks" in result.name:
            errors.extend(self._parse_precommit_errors(output))
        elif "Import Dependencies" in result.name or "Import Smoke" in result.name:
            errors.extend(self._parse_import_errors(output))
        elif "Unit Tests" in result.name:
            errors.extend(self._parse_pytest_errors(output))
        elif "Package Dependencies" in result.name:
            errors.extend(self._parse_pip_errors(output))
        elif "Vulnerability Scan" in result.name:
            if "npm audit" in result.command:
                errors.extend(self._parse_npm_audit_errors(output))
            else:
                errors.extend(self._parse_pip_audit_errors(output))
        elif "Secret Scan" in result.name:
            errors.extend(self._parse_secret_errors(output))
        elif "Working Tree Status" in result.name:
            errors.extend(self._parse_git_status_errors(output))
        elif "Large Binary Check" in result.name:
            errors.extend(self._parse_large_binary_errors(output))
        elif "Design Patterns" in result.name:
            errors.extend(self._parse_pattern_json_errors(output))
        elif "Code Complexity" in result.name:
            errors.extend(self._parse_radon_complexity_errors(output))
        elif "Maintainability Index" in result.name:
            errors.extend(self._parse_radon_maintainability_errors(output))
        elif "Dead Code Analysis" in result.name:
            errors.extend(self._parse_vulture_errors(output))
        elif "Coverage Threshold" in result.name:
            errors.extend(self._parse_coverage_errors(output))

        return errors

    def _parse_ruff_errors(self, output: str) -> List[str]:
        """Parse Ruff linting errors."""
        errors: List[str] = []
        lines = output.split("\n")

        # Tool missing hints
        if "No module named ruff" in output:
            errors.append("**Missing Tool**: `ruff` not installed")
            if "externally-managed-environment" in output:
                errors.append(
                    "**Solution**: Use virtual environment: `python3 -m venv venv && source venv/bin/activate`"
                )
                errors.append("**Alternative**: Install with pipx: `pipx install ruff`")
            else:
                errors.append(
                    "**Solution**: Install ruff: `pip install ruff` or `python3 -m pip install ruff`"
                )
            return errors  # nothing else to parse if tool is missing

        for line in lines:
            if "-->" in line and ":" in line:
                parts = line.split("-->")
                if len(parts) > 1:
                    location = parts[1].strip()
                    errors.append(
                        f"**Linting Error**: Fix code quality issue at `{location}`"
                    )
            elif line.strip().startswith("[") and ("]" in line):
                head, _, tail = line.partition("]")
                error_code = head.strip().lstrip("[")
                description = tail.strip() or line.strip()
                errors.append(f"**{error_code}**: {description}")

        return errors

    def _parse_trunk_errors(self, output: str) -> List[str]:
        """Parse Trunk check errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Trunk format: "file:line:col message [rule]"
            if ":" in line and any(
                keyword in line for keyword in ["error", "warning", "E", "W", "F"]
            ):
                # Extract file and location
                parts = line.split(":", 3)
                if len(parts) >= 3:
                    file_path = parts[0]
                    line_num = parts[1]
                    message = parts[-1] if len(parts) > 3 else line
                    errors.append(
                        f"**Linting Issue**: `{file_path}:{line_num}` - {message.strip()}"
                    )

            # Also catch summary lines
            elif "issues found" in line.lower() or "errors" in line.lower():
                errors.append(f"**Summary**: {line}")

        return errors

    def _parse_mypy_errors(self, output: str) -> List[str]:
        """Parse MyPy type checking errors."""
        errors: List[str] = []
        for line in output.splitlines():
            if "error:" in line.lower() and ":" in line:
                # mypy formats: file:line: column?: error: message
                # we'll conservatively split on 'error:' once
                head, msg = line.split("error:", 1)
                location = head.strip().rstrip(":")
                message = msg.strip()
                if location and message:
                    errors.append(f"**Type Error**: `{location}` - {message}")
        return errors

    def _parse_bandit_errors(self, output: str) -> List[str]:
        """Parse Bandit security errors."""
        errors: List[str] = []
        for line in output.split("\n"):
            s = line.strip()
            if not s:
                continue
            if s.startswith("[B") and "]:" in s:
                parts = s.split("]:", 1)
                if len(parts) == 2:
                    code = parts[0].strip("[").strip()
                    message = parts[1].strip()
                    errors.append(f"**Security Issue**: [{code}] {message}")
            elif ">> Issue:" in s:
                issue = s.replace(">> Issue:", "").strip()
                errors.append(f"**Security Issue**: {issue}")
            elif ".py:" in s and any(k in s.lower() for k in ["high", "medium", "low"]):
                errors.append(f"**Security Issue**: {s}")
        return errors

    def _parse_precommit_errors(self, output: str) -> List[str]:
        """Parse pre-commit hook errors."""
        errors: List[str] = []
        seen_errors: set = set()
        for raw in output.split("\n"):
            line = raw.strip()
            if not line:
                continue

            if ("Failed" in line or "FAILED" in line) and "." in line:
                hook_name = line.split(".", 1)[0].strip()
                if (
                    hook_name
                    and 1 < len(hook_name) < 50
                    and not any(
                        c in hook_name for c in ["❌", "🔍", "+", "-", '"', "$"]
                    )
                ):
                    msg = f"**Hook Failed**: `{hook_name}` - run `pre-commit run {hook_name}` to see details"
                    if msg not in seen_errors:
                        errors.append(msg)
                        seen_errors.add(msg)

            elif (
                line.startswith("src/")
                and ":" in line
                and any(
                    c in line
                    for c in [
                        "D100",
                        "D101",
                        "D102",
                        "D103",
                        "D104",
                        "D105",
                        "D106",
                        "D107",
                    ]
                )
            ):
                parts = line.split(":", 3)
                file_path = parts[0]
                line_num = parts[1] if len(parts) > 1 and parts[1].isdigit() else ""
                error_code = parts[2].strip() if len(parts) > 2 else ""
                location = f"{file_path}:{line_num}" if line_num else file_path
                msg = f"**Docstring**: {error_code} in `{location}`"
                if msg not in seen_errors:
                    errors.append(msg)
                    seen_errors.add(msg)

            elif any(
                k in line.lower() for k in ["error:", "failed:", "traceback"]
            ) and not any(a in line for a in ["+", "-", "stderr", "stdout", "❌", "🔍"]):
                if 10 < len(line) < 200:
                    msg = f"**Pre-commit Error**: {line}"
                    if msg not in seen_errors:
                        errors.append(msg)
                        seen_errors.add(msg)

        if not errors and ("FAILED" in output or "Failed" in output):
            errors.append(
                "**Pre-commit**: Run `pre-commit run --all-files --show-diff-on-failure` for details"
            )

        return errors

    def _parse_import_errors(self, output: str) -> List[str]:
        """Parse import dependency errors."""
        errors: List[str] = []
        seen_errors: set = set()
        for raw in output.split("\n"):
            line = raw.strip()
            if not line:
                continue

            if "Import error in" in line and ":" in line:
                try:
                    parts = line.split("Import error in ", 1)[1]
                    if ":" in parts:
                        module_part, error_part = parts.split(":", 1)
                        module = module_part.strip()
                        error = error_part.strip()
                        msg = f"**Import Error**: `{module}` - {error}"
                        if msg not in seen_errors:
                            errors.append(msg)
                            seen_errors.add(msg)
                except (IndexError, ValueError):
                    if line not in seen_errors:
                        errors.append(f"**Import Error**: {line}")
                        seen_errors.add(line)

            elif "ModuleNotFoundError" in line or "No module named" in line:
                if line not in seen_errors:
                    errors.append(f"**Missing Module**: {line}")
                    seen_errors.add(line)
        return errors

    def _parse_pytest_errors(self, output: str) -> List[str]:
        """Parse pytest test errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            if "FAILED" in line and "::" in line:
                test_path = line.split("FAILED")[0].strip()
                errors.append(f"**Test Failed**: Fix failing test `{test_path}`")
            elif "ERROR" in line and "::" in line:
                test_path = line.split("ERROR")[0].strip()
                errors.append(f"**Test Error**: Fix test error in `{test_path}`")

        return errors

    def _parse_pip_errors(self, output: str) -> List[str]:
        """Parse pip dependency errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            if "has requirement" in line and "but you have" in line:
                errors.append(f"**Dependency Conflict**: {line.strip()}")

        return errors

    def _parse_pip_audit_errors(self, output: str) -> List[str]:
        """Parse pip-audit vulnerability errors."""
        errors: List[str] = []
        current_package: Optional[str] = None
        for raw in output.split("\n"):
            line = raw.strip()
            if not line:
                continue
            if "Found" in line and "vulnerabilit" in line.lower():
                errors.append(f"**Vulnerability Summary**: {line}")
            elif line.startswith("Name:"):
                current_package = line.replace("Name:", "").strip()
            elif line.startswith("Version:") and current_package:
                version = line.replace("Version:", "").strip()
                errors.append(f"**Vulnerable Package**: {current_package} {version}")
            elif "CVE-" in line or "GHSA-" in line:
                errors.append(f"**Vulnerability ID**: {line}")
            elif line.startswith("Description:"):
                desc = line.replace("Description:", "").strip()
                if len(desc) > 100:
                    desc = desc[:100] + "..."
                errors.append(f"**Description**: {desc}")
        return errors

    def _parse_npm_audit_errors(self, output: str) -> List[str]:
        """Parse npm audit vulnerability errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            if "found" in line.lower() and "vulnerabilit" in line.lower():
                errors.append(f"**Vulnerability Summary**: {line.strip()}")
            elif line.strip().startswith("│") and (
                "High" in line or "Critical" in line
            ):
                errors.append(f"**Package Issue**: {line.strip()}")
            elif "Run `npm audit fix`" in line:
                errors.append(f"**Remediation**: {line.strip()}")

        return errors

    def _parse_secret_errors(self, output: str) -> List[str]:
        """Parse secret scanning errors."""
        errors: List[str] = []
        for line in output.split("\n"):
            if ":" in line and any(
                p in line for p in ["AKIA", "ghp_", "sk-", "BEGIN", "AWS_ACCESS"]
            ):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    file_location = f"{parts[0]}:{parts[1]}"
                    errors.append(
                        f"**Secret Found**: Potential credential at `{file_location}`"
                    )
        return errors

    def _parse_git_status_errors(self, output: str) -> List[str]:
        """Parse git status errors."""
        errors: List[str] = []
        try:
            result = subprocess.run(
                ["git", "status", "--short"],  # nosec B603 B607
                capture_output=True,
                text=True,
                cwd=self.project_dir,
            )
            if result.stdout.strip():
                lines = [ln for ln in result.stdout.strip().split("\n") if ln.strip()]
                for line in lines[:5]:
                    status = line[:2]
                    filename = line[3:] if len(line) > 3 else line
                    status_desc = self._get_git_status_description(status)
                    errors.append(f"**Uncommitted**: `{filename}` ({status_desc})")
                if len(lines) > 5:
                    errors.append(
                        f"**Git Status**: {len(lines) - 5} more files have changes - run `git status` to see all"
                    )
            else:
                errors.append("**Git Status**: Working directory clean")
        except Exception:
            errors.append(
                "**Git Status**: Working directory not clean - run `git status` to see changes"
            )
        return errors

    def _get_git_status_description(self, status: str) -> str:
        """Get human-readable description of git status code."""
        status_map = {
            "M ": "Modified",
            " M": "Modified (unstaged)",
            "A ": "Added",
            " A": "Added (unstaged)",
            "D ": "Deleted",
            " D": "Deleted (unstaged)",
            "R ": "Renamed",
            "C ": "Copied",
            "??": "Untracked",
            "MM": "Modified (staged & unstaged)",
            "AM": "Added & Modified",
        }
        return status_map.get(status.strip(), status.strip())

    def _parse_large_binary_errors(self, output: str) -> List[str]:
        """Parse large binary file errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            if "KB)" in line:
                errors.append(f"**Large Binary**: {line.strip()}")

        return errors

    def _parse_pattern_json_errors(self, output: str) -> List[str]:
        """Parse JSON pattern validation errors."""
        errors: List[str] = []
        try:
            data = json.loads(output)
            issues = data.get("issues", [])
            error_issues = [i for i in issues if i.get("severity") == "error"]
            warning_issues = [i for i in issues if i.get("severity") == "warning"]
            for issue in error_issues:
                category = issue.get("category", "unknown").replace("_", " ").title()
                message = issue.get("message", "")
                file_path = issue.get("file_path", "")
                line = issue.get("line_number", 0)
                location = f"{file_path}:{line}" if line else file_path
                errors.append(f"**{category}**: {message} (`{location}`)")
            for issue in warning_issues:
                category = issue.get("category", "unknown").replace("_", " ").title()
                message = issue.get("message", "")
                file_path = issue.get("file_path", "")
                line = issue.get("line_number", 0)
                location = f"{file_path}:{line}" if line else file_path
                errors.append(f"**{category}**: {message} (`{location}`)")
        except (json.JSONDecodeError, KeyError):
            for line in output.splitlines():
                if any(k in line.lower() for k in ["error", "warning", "violation"]):
                    errors.append(f"**Pattern Issue**: {line.strip()}")
        return errors

    def _parse_command_not_found_errors(
        self, output: str, check_name: str
    ) -> List[str]:
        """Parse command-not-found errors and provide remediation."""
        errors: List[str] = []
        for line in output.splitlines():
            lower = line.lower()
            if "command not found" in lower or "not recognized as an internal" in lower:
                if "python" in lower:
                    errors += [
                        "**Missing Command**: `python` not found in PATH",
                        "**Solution**: Install Python or alias `python` to your interpreter (e.g., `alias python=python3`)",
                        "**Alternative**: Use `python3` or activate your virtual environment",
                    ]
                elif "pip" in lower:
                    errors += [
                        "**Missing Command**: `pip` not found in PATH",
                        "**Solution**: Use `python -m pip` (the tool auto-substitutes) or install pip",
                    ]
                elif "npm" in lower:
                    errors += [
                        "**Missing Command**: `npm` not found in PATH",
                        "**Solution**: Install Node.js (includes npm)",
                    ]
                else:
                    missing_cmd = (
                        line.split(":", 1)[0].split("/")[-1].strip()
                        if ":" in line
                        else "unknown"
                    )
                    errors += [
                        f"**Missing Command**: `{missing_cmd}` not found in PATH",
                        f"**Solution**: Install `{missing_cmd}` or update PATH",
                    ]
        if not errors:
            errors = [
                "**Environment Issue**: Required commands not available",
                "**Solution**: Check PATH or activate your virtual environment",
            ]
        return errors

    def _parse_radon_complexity_errors(self, output: str) -> List[str]:
        """Parse Radon complexity errors."""
        errors: List[str] = []
        for line in output.split("\n"):
            if any(rating in line for rating in [" C ", " D ", " E ", " F "]):
                if ":" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        location = parts[0]
                        rating = (
                            parts[-1] if parts[-1] in ["C", "D", "E", "F"] else "High"
                        )
                        errors.append(
                            f"**High Complexity**: `{location}` rated {rating}"
                        )
        return errors

    def _parse_radon_maintainability_errors(self, output: str) -> List[str]:
        """Parse Radon maintainability errors."""
        errors: List[str] = []
        for line in output.splitlines():
            if " - " in line and any(r in line for r in [" C ", " D ", " E ", " F "]):
                parts = line.split(" - ", 1)
                if len(parts) == 2:
                    file_path = parts[0].strip()
                    rating_info = parts[1].strip()
                    errors.append(
                        f"**Low Maintainability**: `{file_path}` - {rating_info}"
                    )
        return errors

    def _parse_vulture_errors(self, output: str) -> List[str]:
        """Parse Vulture dead code errors."""
        errors: List[str] = []
        for line in output.splitlines():
            if any(
                skip in line
                for skip in [
                    "/lib/python",
                    "site-packages",
                    "__pycache__",
                    ".venv",
                    "venv",
                ]
            ):
                continue
            if ":" in line and ("unused" in line.lower() or "dead" in line.lower()):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    location = f"{parts[0]}:{parts[1]}"
                    description = parts[2].strip()
                    if "SyntaxWarning" in description:
                        continue
                    errors.append(f"**Dead Code**: `{location}` - {description}")
        return errors

    def _parse_coverage_errors(self, output: str) -> List[str]:
        """Parse coverage threshold errors."""
        errors = []
        lines = output.split("\n")

        for line in lines:
            if "Coverage:" in line:
                errors.append(f"**Coverage**: {line.strip()}")
            elif "below" in line.lower() and "threshold" in line.lower():
                errors.append(f"**Coverage Issue**: {line.strip()}")

        return errors

    def _since_glob(self, since: Optional[str]) -> str:
        """Get files changed since a git reference, or all files if None."""
        if not since:
            return "."
        try:
            out = (
                subprocess.check_output(  # nosec B603 B607: git command with controlled args
                    ["git", "diff", "--name-only", since, "--", "."],
                    cwd=self.project_dir,
                    text=True,
                )
                .strip()
                .splitlines()
            )
            files = [f for f in out if f.endswith((".py", ".pyi"))]
            return " ".join(files) if files else "."
        except Exception:
            # Fallback to all files if git command fails
            return "."

    def validate(
        self,
        tech_stack: TechStack,
        since: Optional[str] = None,
        category_filter: str = "all",
    ) -> ValidationSummary:
        """Run comprehensive validation for the specified tech stack."""

        # Show category filter info if not running all
        category_info = (
            f" (category: {category_filter})" if category_filter != "all" else ""
        )

        if self.verbose:
            # Verbose header with full details
            print("\n🚀 Refactor Validation Starting...")
            print(f"   Project: {self.project_dir.name}")
            print(f"   Tech: {tech_stack.value}")
            if category_filter != "all":
                print(f"   Category: {category_filter}")
            print(f"   Working Directory: {self.project_dir}")
            print("\n" + "=" * 60)
            print("🏁 VALIDATION PHASE: COMPREHENSIVE")
            print("=" * 60 + "\n")
        else:
            # Minimal header
            print(f"\n🚀 Validating {self.project_dir.name}{category_info}")
            print()

        # Run technology-specific validation - currently Python-focused
        if tech_stack == TechStack.PYTHON:
            results = self.validate_python(since, category_filter)
        # Future language support commented out
        # elif tech_stack == TechStack.JAVASCRIPT:
        #     results = self.validate_javascript(since, category_filter)
        # elif tech_stack == TechStack.JAVA:
        #     results = self.validate_java(since, category_filter)
        # elif tech_stack == TechStack.GO:
        #     results = self.validate_go(since, category_filter)
        # elif tech_stack == TechStack.RUST:
        #     results = self.validate_rust(since, category_filter)
        else:
            # Fallback to Python validation for any unrecognized stack
            results = self.validate_python(since, category_filter)

        # Add pattern validation (will be filtered by category if needed)
        if category_filter == "all" or self.should_run_check(
            "Design Patterns", category_filter
        ):
            pattern_result = self.validate_patterns()
            results.append(pattern_result)

        if self.verbose and category_filter != "all" and len(results) > 0:
            print(f"\n📌 Ran {len(results)} check(s) in category: {category_filter}\n")

        # Calculate summary
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.passed)
        failed_checks = total_checks - passed_checks
        score_percent = (
            int((passed_checks / total_checks) * 100) if total_checks > 0 else 0
        )
        duration = time.time() - self.start_time

        # Show completion summary
        if self.verbose:
            # Verbose completion summary
            print("\n" + "=" * 60)
            print("🏁 VALIDATION COMPLETE")
            print("=" * 60)
            print(f"📊 Final Score: {passed_checks}/{total_checks} ({score_percent}%)")
            print(f"⏱️  Duration: {duration:.1f}s")

            if score_percent >= 90:
                print("🎉 Status: READY TO PROCEED")
            elif score_percent >= 70:
                print("⚠️  Status: NEEDS ATTENTION")
            else:
                print("❌ Status: NOT READY")

            print("=" * 60 + "\n")
        else:
            # Minimal completion summary
            print()
            if score_percent >= 90:
                print(
                    f"🎉 READY TO PROCEED - {passed_checks}/{total_checks} checks passed ({score_percent}%) in {duration:.1f}s"
                )
            elif score_percent >= 70:
                print(
                    f"⚠️  NEEDS ATTENTION - {passed_checks}/{total_checks} checks passed ({score_percent}%) in {duration:.1f}s"
                )
            else:
                print(
                    f"❌ NOT READY - {passed_checks}/{total_checks} checks passed ({score_percent}%) in {duration:.1f}s"
                )
            print()

        # Calculate enhanced reporting fields
        layer_summaries = self.calculate_layer_summaries(results)
        production_readiness = self.assess_production_readiness(results, score_percent)

        # Determine confidence and risk
        confidence = (
            "High"
            if score_percent >= 90
            else "Medium" if score_percent >= 70 else "Low"
        )
        risk_level = (
            "Low"
            if score_percent >= 90
            else "Medium" if score_percent >= 70 else "High"
        )

        # Create final recommendation
        final_recommendation = {
            "status": (
                "READY TO PROCEED"
                if score_percent >= 90
                else "NEEDS ATTENTION" if score_percent >= 70 else "NOT READY"
            ),
            "reasoning": f"Validation score of {score_percent}% indicates {'excellent' if score_percent >= 90 else 'acceptable' if score_percent >= 70 else 'insufficient'} code quality",
            "next_steps": [
                (
                    "Address failing checks marked with ❌"
                    if failed_checks > 0
                    else "All checks passed"
                ),
                (
                    "Re-run validation after fixes"
                    if failed_checks > 0
                    else "Ready for deployment"
                ),
                (
                    "Consider additional manual testing"
                    if score_percent < 90
                    else "Automated validation complete"
                ),
            ],
            "confidence": confidence,
            "risk": risk_level,
        }

        return ValidationSummary(
            tech_stack=tech_stack,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            score_percent=score_percent,
            duration=duration,
            results=results,
            project_name=self.project_dir.name,
            layer_summaries=layer_summaries,
            production_readiness=production_readiness,
            confidence=confidence,
            risk_level=risk_level,
            final_recommendation=final_recommendation,
        )

    def generate_report(
        self, summary: ValidationSummary, output_file: Optional[str] = None
    ) -> str:
        """Generate human-readable validation report."""
        report = f"""# 🔍 Refactor Validation Report

**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Mode**: Comprehensive
**Tech Stack**: {summary.tech_stack.value}
**Project**: {self.project_dir.name}
**Duration**: {summary.duration:.1f}s

## ✅ Results Summary

**Score**: {summary.passed_checks}/{summary.total_checks} ({summary.score_percent}%)

| Check | Status | Message |
|-------|--------|---------|
"""
        for result in summary.results:
            status_icon = "✅" if result.passed else "❌"
            required_mark = "❌" if result.required and not result.passed else ""
            report += f"| {result.name} | {status_icon} {required_mark} | {result.message} |\n"

        if summary.score_percent >= 90:
            assessment = """
## 🎉 Assessment: READY TO PROCEED

**Risk Level**: Low

**Status**: All checks passed - ready for deployment
"""
        elif summary.score_percent >= 70:
            actionable_errors = self.extract_actionable_errors(summary.results)
            issues_list = _format_issues_list(actionable_errors, "medium")
            assessment = f"""
## ⚠️ Assessment: NEEDS ATTENTION

**Risk Level**: Medium

**Issues to Fix**:
{issues_list}
"""
        else:
            actionable_errors = self.extract_actionable_errors(summary.results)
            issues_list = _format_issues_list(actionable_errors, "high")
            assessment = f"""
## ❌ Assessment: NOT READY

**Risk Level**: High

**Issues to Fix**:
{issues_list}
"""

        report += assessment

        if output_file:
            Path(output_file).write_text(report)
            print(f"📄 Report saved to: {output_file}")

        return report


def _format_issues_list(actionable_errors: List[str], risk_level: str) -> str:
    """Format actionable errors into a numbered list of issues to fix."""
    if not actionable_errors:
        if risk_level == "high":
            return "1. Check validation output above for specific error details\n2. Address critical failures first (marked with X)"
        else:
            return "1. Check validation output above for specific error details"

    steps = []

    # Add specific errors
    for i, error in enumerate(actionable_errors, 1):
        steps.append(f"{i}. {error}")

    # No redundant follow-up steps needed

    return "\n".join(steps)


def main():
    parser = argparse.ArgumentParser(
        description="Refactor Validation Tool - Validate code safety after refactoring"
    )
    parser.add_argument(
        "project_dir",
        nargs="?",
        default=".",
        help="Project directory to validate (default: current directory)",
    )
    parser.add_argument(
        "--tech",
        type=str,
        choices=["python", "javascript", "java", "go", "rust", "generic"],
        default="python",
        help="Technology stack (default: python)",
    )
    parser.add_argument(
        "--output", type=str, help="Save report to file (default: print to console)"
    )
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero on NEEDS ATTENTION (CI-friendly)",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Restrict checks to files changed since git ref (performance optimization)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output including commands and full error messages (default: minimal)",
    )
    parser.add_argument(
        "--category",
        type=str,
        choices=[
            "code-quality",
            "security",
            "tests",
            "dependencies",
            "dead-code",
            "patterns",
            "documentation",
            "other",
            "all",
        ],
        default="all",
        help="Run checks for a specific category only (default: all)",
    )

    args = parser.parse_args()

    # Auto-enable verbose mode when filtering by category
    if args.category != "all" and not args.verbose:
        args.verbose = True
        print("ℹ️  Verbose mode auto-enabled for category filtering\n")

    # Convert string arguments to enums
    tech_stack = TechStack(args.tech)

    # Run validation (always uses standard/comprehensive mode)
    validator = RefactorValidator(args.project_dir, verbose=args.verbose)
    summary = validator.validate(
        tech_stack, since=args.since, category_filter=args.category
    )

    # Output results with comprehensive JSON structure
    if args.json:
        result_dict = {
            "timestamp": summary.timestamp,
            "mode": "comprehensive",
            "tech_stack": summary.tech_stack.value,
            "project_name": summary.project_name,
            "score_percent": summary.score_percent,
            "passed_checks": summary.passed_checks,
            "total_checks": summary.total_checks,
            "duration": summary.duration,
            "confidence": summary.confidence,
            "risk_level": summary.risk_level,
            "production_readiness": {
                "functionality": summary.production_readiness.functionality,
                "reliability": summary.production_readiness.reliability,
                "performance": summary.production_readiness.performance,
                "maintainability": summary.production_readiness.maintainability,
                "security": summary.production_readiness.security,
                "evidence": summary.production_readiness.evidence,
            },
            "layer_summaries": [
                {
                    "layer": layer.layer.value,
                    "checks_passed": layer.checks_passed,
                    "total_checks": layer.total_checks,
                    "score_percent": layer.score_percent,
                    "status": layer.status,
                }
                for layer in summary.layer_summaries
            ],
            "final_recommendation": summary.final_recommendation,
            "validation_results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "message": r.message,
                    "duration": r.duration,
                    "required": r.required,
                    "layer": r.layer.value if r.layer else None,
                    "remediation_tip": r.remediation_tip,
                }
                for r in summary.results
            ],
        }

        if args.output:
            Path(args.output).write_text(json.dumps(result_dict, indent=2))
            print(f"📄 JSON report saved to: {args.output}")
        else:
            print(json.dumps(result_dict, indent=2))
    else:
        # Generate and display report (verbose mode or when saving to file)
        if args.verbose or args.output:
            report = validator.generate_report(summary, args.output)
            if not args.output:
                print(report)
        else:
            # Minimal mode - always show actionable fixes if there are any failures
            failed_results = [r for r in summary.results if not r.passed]
            if failed_results:
                categorized_errors = validator.extract_categorized_errors(
                    summary.results
                )
                print("\n🔧 Issues to Fix:")
                if categorized_errors:
                    category_filter_name = {
                        "Code Quality": "code-quality",
                        "Security": "security",
                        "Dependencies": "dependencies",
                        "Tests": "tests",
                        "Dead Code": "dead-code",
                        "Patterns": "patterns",
                        "Documentation": "documentation",
                        "Other": "other",
                    }
                    category_icons = {
                        "Code Quality": "📝",
                        "Security": "🔒",
                        "Dependencies": "📦",
                        "Tests": "🧪",
                        "Dead Code": "🧹",
                        "Patterns": "🎨",
                        "Documentation": "📚",
                        "Other": "⚠️",
                    }
                    for category, data in categorized_errors.items():
                        errors = data["errors"]
                        commands = data["commands"]
                        icon = category_icons.get(category, "•")
                        count = len(errors)
                        print(
                            f"\n{icon} {category} ({count} issue{'s' if count != 1 else ''}):"
                        )
                        max_display = 10
                        for error in errors[:max_display]:
                            cleaned_error = validator._truncate_long_text(
                                error, max_length=200
                            )
                            print(f"  • {cleaned_error}")
                        cli_category = category_filter_name.get(
                            category, category.lower()
                        )
                        script_name = (
                            "python refactor_toolkit/validate.py"
                            if not hasattr(sys, "_MEIPASS")
                            else "validate"
                        )
                        if count > max_display:
                            remaining = count - max_display
                            print(f"  • ... and {remaining} more issue(s)")
                            print(
                                f"  💡 Run: {script_name} . --category {cli_category}"
                            )
                        elif commands:
                            print(
                                f"  💡 Run: {script_name} . --category {cli_category}"
                            )
                else:
                    # Fallback to basic error list if no actionable errors parsed
                    for i, result in enumerate(failed_results, 1):
                        print(f"{i:2d}. {result.name}: {result.message}")

                print("\n💡 Run with --verbose for detailed error information")
                print("💡 Run with --output report.md to save full report")

    # Print detailed summary to console (only in verbose mode or JSON mode)
    if args.verbose or args.json:
        print(
            f"\n📊 Final Score: {summary.passed_checks}/{summary.total_checks} ({summary.score_percent}%)"
        )
        print(f"⏱️  Duration: {summary.duration:.1f}s")

        if summary.score_percent >= 90:
            print("🎉 Status: READY TO PROCEED")
        elif summary.score_percent >= 70:
            print("⚠️  Status: NEEDS ATTENTION")
        else:
            print("❌ Status: NOT READY")

    # Determine exit code based on strict mode
    exit_code = 0
    if summary.score_percent >= 90:
        exit_code = 0
    elif summary.score_percent >= 70:
        exit_code = (
            2 if args.strict else 0
        )  # CI-friendly: fail on NEEDS ATTENTION if --strict
    else:
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
