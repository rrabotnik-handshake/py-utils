#!/bin/bash
# Quick Validation Script (5-10 minutes)
# Universal Refactor Validation Toolkit

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="${1:-.}"
TECH_STACK="${2:-auto}"
REPORT_FILE="validation_quick_$(date +%Y%m%d_%H%M%S).md"

echo -e "${BLUE}🚀 Quick Validation Starting...${NC}"
echo "Project: $PROJECT_DIR"
echo "Tech Stack: $TECH_STACK"
echo "Report: $REPORT_FILE"
echo ""

# Initialize report
cat > "$REPORT_FILE" << EOF
# 📊 Quick Validation Report

**Date**: $(date)
**Project**: $(basename "$PROJECT_DIR")
**Tech Stack**: $TECH_STACK
**Validation Time**: 5-10 minutes

## ✅ Validation Results

| **Check** | **Status** | **Notes** |
|-----------|------------|-----------|
EOF

# Function to add result to report
add_result() {
    local check="$1"
    local status="$2" 
    local notes="$3"
    echo "| $check | $status | $notes |" >> "$REPORT_FILE"
}

# Function to run command and capture result
run_check() {
    local name="$1"
    local cmd="$2"
    local success_msg="$3"
    local fail_msg="$4"
    
    echo -e "${YELLOW}Checking: $name${NC}"
    
    if eval "$cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ $name: PASSED${NC}"
        add_result "$name" "✅" "$success_msg"
        return 0
    else
        echo -e "${RED}❌ $name: FAILED${NC}"
        add_result "$name" "❌" "$fail_msg"
        return 1
    fi
}

# Auto-detect technology if not specified
if [ "$TECH_STACK" = "auto" ]; then
    if [ -f "package.json" ]; then
        TECH_STACK="javascript"
    elif [ -f "requirements.txt" ] || [ -f "pyproject.toml" ] || [ -f "setup.py" ]; then
        TECH_STACK="python"
    elif [ -f "pom.xml" ] || [ -f "build.gradle" ]; then
        TECH_STACK="java"
    elif [ -f "go.mod" ]; then
        TECH_STACK="go"
    elif [ -f "Cargo.toml" ]; then
        TECH_STACK="rust"
    else
        TECH_STACK="generic"
    fi
    echo "Auto-detected tech stack: $TECH_STACK"
fi

# Track results
TOTAL_CHECKS=0
PASSED_CHECKS=0

# Technology-specific validation
case "$TECH_STACK" in
    "python")
        echo -e "${BLUE}🐍 Python Validation${NC}"
        
        # Code quality
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Code Quality" "python -m ruff check $PROJECT_DIR" "No linting issues" "Linting issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Type checking
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Type Checking" "python -m mypy $PROJECT_DIR" "Type checking passed" "Type errors found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Tests
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Unit Tests" "python -m pytest tests/ -x" "All tests passed" "Test failures found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Import check
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Import Check" "python -c 'import sys; sys.path.insert(0, \"$PROJECT_DIR\"); import $(basename $PROJECT_DIR)'" "Imports work" "Import errors"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
        
    "javascript")
        echo -e "${BLUE}📦 JavaScript/Node.js Validation${NC}"
        
        # Linting
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "ESLint" "npx eslint $PROJECT_DIR" "No linting issues" "Linting issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Type checking
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "TypeScript" "npx tsc --noEmit" "Type checking passed" "Type errors found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Tests
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Unit Tests" "npm test" "All tests passed" "Test failures found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Build check
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Build Check" "npm run build" "Build successful" "Build failed"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
        
    "java")
        echo -e "${BLUE}☕ Java Validation${NC}"
        
        # Compilation
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Compilation" "mvn compile -q" "Compilation successful" "Compilation failed"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Tests
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Unit Tests" "mvn test -q" "All tests passed" "Test failures found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Code style
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Code Style" "mvn checkstyle:check -q" "Style checks passed" "Style issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
        
    "go")
        echo -e "${BLUE}🐹 Go Validation${NC}"
        
        # Format check
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Format Check" "gofmt -l $PROJECT_DIR | wc -l | grep -q '^0$'" "Code properly formatted" "Formatting issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Vet
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Go Vet" "go vet ./..." "No vet issues" "Vet issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Tests
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Unit Tests" "go test ./..." "All tests passed" "Test failures found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Build
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Build Check" "go build ./..." "Build successful" "Build failed"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
        
    "rust")
        echo -e "${BLUE}🦀 Rust Validation${NC}"
        
        # Format check
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Format Check" "cargo fmt -- --check" "Code properly formatted" "Formatting issues found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Clippy
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Clippy" "cargo clippy -- -D warnings" "No clippy warnings" "Clippy warnings found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Tests
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Unit Tests" "cargo test" "All tests passed" "Test failures found"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # Build
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Build Check" "cargo build" "Build successful" "Build failed"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
        
    *)
        echo -e "${YELLOW}⚠️  Generic validation (no specific tech stack detected)${NC}"
        
        # Git status
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "Git Status" "git status --porcelain | wc -l | grep -q '^0$'" "Working directory clean" "Uncommitted changes"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        
        # File permissions
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        if run_check "File Permissions" "find $PROJECT_DIR -name '*.sh' -executable | wc -l | grep -q '[0-9]'" "Executable files found" "No executable files"; then
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
        fi
        ;;
esac

# Pattern validation if available
if [ -f "validate_patterns.py" ]; then
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    if run_check "Design Patterns" "python validate_patterns.py $PROJECT_DIR --auto-detect" "Pattern validation passed" "Pattern issues found"; then
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
    fi
fi

# Calculate score
SCORE_PERCENT=$((PASSED_CHECKS * 100 / TOTAL_CHECKS))

# Complete report
cat >> "$REPORT_FILE" << EOF

## 📊 Quick Assessment

**Checks Passed**: $PASSED_CHECKS/$TOTAL_CHECKS ($SCORE_PERCENT%)

EOF

# Determine status
if [ $SCORE_PERCENT -ge 90 ]; then
    STATUS="✅ READY"
    RISK="Low"
    echo -e "${GREEN}🎉 Validation Complete: READY TO PROCEED${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ✅ READY
**Risk Level**: Low
**Recommendation**: Proceed with confidence

**Next Steps**:
1. Continue with deployment/merge
2. Monitor for any issues
3. Consider comprehensive validation for major changes
EOF
elif [ $SCORE_PERCENT -ge 70 ]; then
    STATUS="⚠️ NEEDS ATTENTION"
    RISK="Medium"
    echo -e "${YELLOW}⚠️  Validation Complete: NEEDS ATTENTION${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ⚠️ NEEDS ATTENTION
**Risk Level**: Medium
**Recommendation**: Address issues before proceeding

**Next Steps**:
1. Fix failing checks above
2. Re-run validation
3. Consider additional testing
EOF
else
    STATUS="❌ NOT READY"
    RISK="High"
    echo -e "${RED}❌ Validation Complete: NOT READY${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ❌ NOT READY
**Risk Level**: High
**Recommendation**: Do not proceed until issues are resolved

**Next Steps**:
1. Address all failing checks
2. Run comprehensive validation
3. Consider code review
EOF
fi

echo ""
echo "📊 Final Score: $PASSED_CHECKS/$TOTAL_CHECKS ($SCORE_PERCENT%)"
echo "📄 Report saved to: $REPORT_FILE"
echo ""

# Exit with appropriate code
if [ $SCORE_PERCENT -ge 70 ]; then
    exit 0
else
    exit 1
fi
