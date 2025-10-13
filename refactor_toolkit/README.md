# 🛠️ Refactor Validation Toolkit

**A simple, focused tool to validate code safety after refactoring.**

Answers one question: **"Is my code safe to deploy after this change?"**

## 🚀 Quick Start

```bash
# Quick validation (5-10 minutes) - recommended for daily use
./validate

# Comprehensive validation (15-30 minutes) - for major changes
./validate --mode standard

# Verbose output with detailed commands and error messages
./validate --verbose

# Save report to file
./validate --output report.md

# Get JSON output for CI/CD
./validate --json --output results.json
```

## 🎯 What It Does

**Essential Checks (All Modes):**

- ✅ **Syntax validation** - Code compiles/parses correctly
- ✅ **Basic functionality** - Critical systems still work

**Quick Mode Adds:**

- ✅ **Code quality** - Linting passes
- ✅ **Import checks** - Dependencies resolve

**Standard Mode Adds:**

- ✅ **Type checking** - Static analysis passes
- ✅ **Pre-commit hooks** - All configured hooks pass
- ✅ **Unit tests** - Existing tests still pass
- ✅ **Performance checks** - Giant files and I/O risk detection
- ✅ **CI awareness** - Tool versions and environment parity

## 🔧 Supported Technologies

| Language   | Auto-Detection                       | Checks                                        |
| ---------- | ------------------------------------ | --------------------------------------------- |
| **Python** | `pyproject.toml`, `requirements.txt` | syntax, ruff/flake8, mypy, pre-commit, pytest |

> **Note**: Currently focused on Python projects. Support for JavaScript, Java, Go, and Rust is planned for future releases.

## 📊 Results

The tool gives you a simple **pass/fail score**:

- **90-100%**: ✅ **READY** - Safe to deploy
- **70-89%**: ⚠️ **NEEDS ATTENTION** - Fix issues first
- **<70%**: ❌ **NOT READY** - Do not deploy

## 🔧 Usage Examples

```bash
# Basic usage
./validate                          # Auto-detect tech, quick mode
python validate.py                  # Same as above

# Specify technology
./validate --tech python           # Force Python validation
./validate --tech javascript       # Force JavaScript validation

# Different modes
./validate --mode emergency        # 30-second critical checks
./validate --mode quick            # 5-10 minute essential checks (default)
./validate --mode standard         # 15-30 minute comprehensive

# Output options
./validate --output report.md      # Human-readable report
./validate --json                  # JSON to stdout
./validate --json --output ci.json # JSON to file for CI/CD

# Validate different directory
./validate /path/to/project --mode standard
```

## 🏗️ CI/CD Integration

```yaml
# GitHub Actions example
- name: Validate Refactor
  run: |
    cd refactor_toolkit
    ./validate --mode standard --json --output validation.json

# Exit codes:
# 0 = Ready (≥70% pass rate)
# 1 = Not ready (<70% pass rate)
```

## 📊 Output Modes

**Default (Minimal)**:

- Clean, focused output showing only essential information
- One-line status per check with pass/fail and duration
- Actionable list of issues to fix (when validation fails)
- Perfect for daily development workflow

**Verbose (`--verbose`)**:

- Detailed output with commands being executed
- Full error messages and debugging information
- Complete validation report with all sections
- Ideal for troubleshooting and CI/CD pipelines

**Examples**:

```bash
# Minimal output (default) - successful run
🔍 Python Syntax... ✅ (0.7s)
🔍 Code Quality... ✅ (0.8s)
🎉 READY TO PROCEED - 22/22 checks passed (100%) in 45.2s

# Minimal output (default) - with issues
🔍 Type Checking... ❌ FAILED (0.3s)
🔍 Security Scan... ❌ FAILED (0.2s)
❌ NOT READY - 20/22 checks passed (90%) in 45.2s

🔧 Issues to Fix:
 1. **Type Error**: Fix type issue in `src/main.py:42` - incompatible types
 2. **Security Issue**: [B602] subprocess call with shell=True at `validate.py:205`
 3. **Hook Failed**: `pydocstyle` - run `pre-commit run pydocstyle` to see details
 4. **Pre-commit Error**: D107: Missing docstring in __init__
 5. **Unit Tests**: Test failures detected → Run `pytest -vv` and fix top failures first

💡 Run with --verbose for detailed error information

# Verbose output
🔍 Running: Python Syntax
   Command: python -m compileall -q .
   Result: ✅ Python Syntax (REQUIRED) - 0.7s
   ✅ All Python files compile successfully
```

## 🚀 Performance & CI Checks

**Performance Smoke Tests** (Standard Mode):

- **Giant Files**: Detects Python files >800 LOC that may need refactoring
- **I/O Risk Patterns**: Identifies potential N+1 queries or heavy I/O in loops
- **Tool Versions**: Captures Python, pytest, mypy, ruff versions for reproducibility

**Examples**:

```bash
# Giant file detected
❌ Giant Files: src/monolith.py (1,247 lines) - consider breaking into modules

# I/O risk detected
❌ I/O Risk Patterns: requests.get() inside for loop detected in api_client.py:42

# Tool versions captured
✅ Tool Versions: Python 3.11.5, pytest 7.4.2, mypy 1.5.1, ruff 0.0.287
```

## ⚙️ Configuration

Simple configuration in `config.yaml`:

```yaml
modes:
  emergency:
    description: "Critical checks only (30 seconds)"
    time_limit: 30
  quick:
    description: "Essential validation (5-10 minutes)"
    time_limit: 600
  standard:
    description: "Comprehensive validation (15-30 minutes)"
    time_limit: 1800

thresholds:
  ready: 90 # ≥90% = Ready to proceed
  attention: 70 # 70-89% = Needs attention
```

## 📁 Project Structure

```
refactor_toolkit/
├── validate.py          # Main validation tool (Python)
├── validate             # Bash wrapper script
├── config.yaml          # Simple configuration
├── README.md            # This file
└── output/              # Generated reports (gitignored)
    └── reports/
```

## 🎯 Key Features

**Clean, Actionable Output:**

- Minimal mode shows only essential information with specific fixes to make
- No duplicate or redundant information
- Progressive disclosure: `--verbose` for detailed debugging when needed

**Comprehensive Validation:**

- 22 validation checks across 7 layers (Code Quality, Security, Performance, etc.)
- Performance smoke tests (giant files, I/O patterns, tool versions)
- Advanced pattern analysis (design patterns, code complexity, dead code)

**Developer-Friendly:**

- Terminal-agnostic execution (works with `python`, `python3`, `py`)
- Smart tool integration (Trunk, pre-commit, MyPy, Ruff)
- Actionable error messages with remediation tips

## 🎯 Philosophy

**Simple & Focused:**

- One clear purpose: validate refactor safety
- No feature creep or over-engineering
- Fast feedback for developers

**Technology Agnostic:**

- Auto-detects your tech stack
- Runs appropriate checks for each language
- Graceful fallback for unknown projects

**CI/CD Ready:**

- Clear exit codes
- JSON output for automation
- Configurable time limits

## 🚀 Getting Started

1. **Make executable**: `chmod +x validate`
2. **Run validation**: `./validate`
3. **Check the results**: Look for ✅ READY, ⚠️ NEEDS ATTENTION, or ❌ NOT READY
4. **Fix issues if needed**: Address any failing checks
5. **Re-run**: `./validate` until you get ✅ READY

That's it! Simple, focused, effective.

---

_This toolkit was built for the schema-diff project but works with any codebase. It focuses on answering one question well: "Is my code safe to deploy after this change?"_
