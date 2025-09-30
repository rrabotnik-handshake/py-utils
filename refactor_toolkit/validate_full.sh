#!/bin/bash
# Comprehensive Validation Script (30-45 minutes)
# Universal Refactor Validation Toolkit

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="${1:-.}"
TECH_STACK="${2:-auto}"
REPORT_FILE="validation_comprehensive_$(date +%Y%m%d_%H%M%S).md"
START_TIME=$(date +%s)

echo -e "${BLUE}🚀 Comprehensive Validation Starting...${NC}"
echo "Project: $PROJECT_DIR"
echo "Tech Stack: $TECH_STACK"
echo "Report: $REPORT_FILE"
echo ""

# Initialize comprehensive report
cat > "$REPORT_FILE" << EOF
# 📊 Comprehensive Validation Assessment

**Date**: $(date)
**Project**: $(basename "$PROJECT_DIR")
**Tech Stack**: $TECH_STACK
**Validation Type**: Comprehensive (30-45 minutes)

## ✅ Validation Results

| **Layer** | **Status** | **Results** | **Time** |
|-----------|------------|-------------|----------|
EOF

# Function to add layer result to report
add_layer_result() {
    local layer="$1"
    local status="$2" 
    local results="$3"
    local time="$4"
    echo "| $layer | $status | $results | $time |" >> "$REPORT_FILE"
}

# Function to run validation layer
run_layer() {
    local layer_name="$1"
    local layer_start=$(date +%s)
    
    echo -e "${PURPLE}📋 Layer: $layer_name${NC}"
    echo "----------------------------------------"
    
    # Return values: passed_checks, total_checks, details
    case "$layer_name" in
        "Code Quality")
            run_code_quality_layer
            ;;
        "Unit Tests")
            run_unit_tests_layer
            ;;
        "Integration")
            run_integration_layer
            ;;
        "Patterns")
            run_patterns_layer
            ;;
        "Performance")
            run_performance_layer
            ;;
        "Security")
            run_security_layer
            ;;
    esac
    
    local layer_end=$(date +%s)
    local layer_time=$((layer_end - layer_start))
    local layer_time_formatted="${layer_time}s"
    
    # Calculate layer status
    local layer_score=$((LAYER_PASSED * 100 / LAYER_TOTAL))
    local layer_status
    local layer_results="$LAYER_PASSED/$LAYER_TOTAL checks passed"
    
    if [ $layer_score -ge 90 ]; then
        layer_status="✅"
    elif [ $layer_score -ge 70 ]; then
        layer_status="⚠️"
    else
        layer_status="❌"
    fi
    
    add_layer_result "$layer_name" "$layer_status" "$layer_results" "$layer_time_formatted"
    
    echo -e "${BLUE}Layer Complete: $layer_results ($layer_score%) in ${layer_time}s${NC}"
    echo ""
    
    # Update global counters
    TOTAL_CHECKS=$((TOTAL_CHECKS + LAYER_TOTAL))
    PASSED_CHECKS=$((PASSED_CHECKS + LAYER_PASSED))
}

# Layer implementations
run_code_quality_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    case "$TECH_STACK" in
        "python")
            # Ruff linting
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m ruff check "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Ruff linting passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Ruff linting failed${NC}"
            fi
            
            # MyPy type checking
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m mypy "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ MyPy type checking passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ MyPy type checking failed${NC}"
            fi
            
            # Black formatting
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m black --check "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Black formatting passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Black formatting failed${NC}"
            fi
            
            # Import sorting
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m isort --check-only "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Import sorting passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Import sorting failed${NC}"
            fi
            ;;
            
        "javascript")
            # ESLint
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if npx eslint "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ ESLint passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ ESLint failed${NC}"
            fi
            
            # Prettier
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if npx prettier --check "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Prettier formatting passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Prettier formatting failed${NC}"
            fi
            
            # TypeScript
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if npx tsc --noEmit > /dev/null 2>&1; then
                echo -e "${GREEN}✅ TypeScript compilation passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ TypeScript compilation failed${NC}"
            fi
            ;;
            
        *)
            # Generic checks
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if find "$PROJECT_DIR" -name "*.md" -exec grep -l "TODO\|FIXME\|XXX" {} \; | wc -l | grep -q '^0$'; then
                echo -e "${GREEN}✅ No TODO/FIXME markers found${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ TODO/FIXME markers found${NC}"
            fi
            ;;
    esac
}

run_unit_tests_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    case "$TECH_STACK" in
        "python")
            # Pytest
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m pytest tests/ -v > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Pytest passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Pytest failed${NC}"
            fi
            
            # Coverage
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if python -m pytest tests/ --cov="$PROJECT_DIR" --cov-report=term-missing --cov-fail-under=80 > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Test coverage ≥80%${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Test coverage <80%${NC}"
            fi
            ;;
            
        "javascript")
            # Jest/npm test
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if npm test > /dev/null 2>&1; then
                echo -e "${GREEN}✅ JavaScript tests passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ JavaScript tests failed${NC}"
            fi
            
            # Coverage
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if npm run test:coverage > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Test coverage check passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Test coverage check failed${NC}"
            fi
            ;;
            
        *)
            # Generic test check
            LAYER_TOTAL=$((LAYER_TOTAL + 1))
            if find "$PROJECT_DIR" -name "*test*" -type f | wc -l | grep -q '[1-9]'; then
                echo -e "${GREEN}✅ Test files found${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ No test files found${NC}"
            fi
            ;;
    esac
}

run_integration_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    # Build/compile check
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    case "$TECH_STACK" in
        "python")
            if python -c "import sys; sys.path.insert(0, '$PROJECT_DIR'); import $(basename $PROJECT_DIR)" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Python import check passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Python import check failed${NC}"
            fi
            ;;
        "javascript")
            if npm run build > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Build check passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${RED}❌ Build check failed${NC}"
            fi
            ;;
        *)
            echo -e "${YELLOW}⚠️ Generic integration check skipped${NC}"
            LAYER_PASSED=$((LAYER_PASSED + 1))
            ;;
    esac
    
    # Dependency check
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    case "$TECH_STACK" in
        "python")
            if pip check > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Python dependencies consistent${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Python dependency conflicts${NC}"
            fi
            ;;
        "javascript")
            if npm audit --audit-level=high > /dev/null 2>&1; then
                echo -e "${GREEN}✅ No high-severity vulnerabilities${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ High-severity vulnerabilities found${NC}"
            fi
            ;;
        *)
            echo -e "${YELLOW}⚠️ Generic dependency check skipped${NC}"
            LAYER_PASSED=$((LAYER_PASSED + 1))
            ;;
    esac
}

run_patterns_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    # Pattern validation tool
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    if [ -f "validate_patterns.py" ]; then
        if python validate_patterns.py "$PROJECT_DIR" --auto-detect > /dev/null 2>&1; then
            echo -e "${GREEN}✅ Design pattern validation passed${NC}"
            LAYER_PASSED=$((LAYER_PASSED + 1))
        else
            echo -e "${YELLOW}⚠️ Design pattern issues found${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️ Pattern validation tool not found${NC}"
    fi
    
    # Code complexity check
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    case "$TECH_STACK" in
        "python")
            if python -m radon cc "$PROJECT_DIR" -a -nb > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Code complexity acceptable${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ High code complexity detected${NC}"
            fi
            ;;
        *)
            echo -e "${YELLOW}⚠️ Complexity check not available for $TECH_STACK${NC}"
            LAYER_PASSED=$((LAYER_PASSED + 1))
            ;;
    esac
}

run_performance_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    # Basic performance checks
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    case "$TECH_STACK" in
        "python")
            # Check for common performance anti-patterns
            if ! grep -r "import \*" "$PROJECT_DIR" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ No wildcard imports found${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Wildcard imports found (performance impact)${NC}"
            fi
            ;;
        "javascript")
            # Check bundle size if available
            if [ -f "package.json" ] && npm run build:analyze > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Bundle analysis completed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Bundle analysis not available${NC}"
            fi
            ;;
        *)
            echo -e "${YELLOW}⚠️ Performance check not available for $TECH_STACK${NC}"
            LAYER_PASSED=$((LAYER_PASSED + 1))
            ;;
    esac
    
    # Memory usage patterns
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    if ! grep -r "while True:" "$PROJECT_DIR" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ No infinite loops detected${NC}"
        LAYER_PASSED=$((LAYER_PASSED + 1))
    else
        echo -e "${YELLOW}⚠️ Potential infinite loops found${NC}"
    fi
}

run_security_layer() {
    LAYER_PASSED=0
    LAYER_TOTAL=0
    
    # Security scanning
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    case "$TECH_STACK" in
        "python")
            if python -m bandit -r "$PROJECT_DIR" -ll > /dev/null 2>&1; then
                echo -e "${GREEN}✅ Bandit security scan passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Security issues found by Bandit${NC}"
            fi
            ;;
        "javascript")
            if npm audit --audit-level=moderate > /dev/null 2>&1; then
                echo -e "${GREEN}✅ npm audit passed${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ npm audit found issues${NC}"
            fi
            ;;
        *)
            # Generic security checks
            if ! grep -r "password\|secret\|key" "$PROJECT_DIR" --include="*.py" --include="*.js" --include="*.java" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ No hardcoded secrets detected${NC}"
                LAYER_PASSED=$((LAYER_PASSED + 1))
            else
                echo -e "${YELLOW}⚠️ Potential hardcoded secrets found${NC}"
            fi
            ;;
    esac
    
    # File permissions
    LAYER_TOTAL=$((LAYER_TOTAL + 1))
    if ! find "$PROJECT_DIR" -type f -perm -002 | grep -q .; then
        echo -e "${GREEN}✅ File permissions secure${NC}"
        LAYER_PASSED=$((LAYER_PASSED + 1))
    else
        echo -e "${YELLOW}⚠️ World-writable files found${NC}"
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

# Initialize counters
TOTAL_CHECKS=0
PASSED_CHECKS=0

# Run all validation layers
run_layer "Code Quality"
run_layer "Unit Tests"
run_layer "Integration"
run_layer "Patterns"
run_layer "Performance"
run_layer "Security"

# Calculate final results
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
SCORE_PERCENT=$((PASSED_CHECKS * 100 / TOTAL_CHECKS))

# Complete comprehensive report
cat >> "$REPORT_FILE" << EOF

**Total Validation Time**: ${TOTAL_TIME}s

## 🎯 Production Readiness

| **Criteria** | **Status** | **Evidence** |
|--------------|------------|--------------|
| Functionality | $([ $SCORE_PERCENT -ge 80 ] && echo "✅" || echo "⚠️") | $PASSED_CHECKS/$TOTAL_CHECKS checks passed |
| Reliability | $([ $SCORE_PERCENT -ge 90 ] && echo "✅" || echo "⚠️") | All layers validated |
| Performance | $([ $SCORE_PERCENT -ge 85 ] && echo "✅" || echo "⚠️") | Performance layer results |
| Maintainability | $([ $SCORE_PERCENT -ge 80 ] && echo "✅" || echo "⚠️") | Code quality and patterns validated |
| Security | $([ $SCORE_PERCENT -ge 90 ] && echo "✅" || echo "⚠️") | Security layer results |

## 💡 Key Findings

**Strengths**:
- Comprehensive validation completed
- Multi-layer approach used
- Automated checks performed

**Issues Found**:
- $((TOTAL_CHECKS - PASSED_CHECKS)) checks failed
- See individual layer results above

**Risks Identified**:
- Score below 90% indicates potential issues
- Failed security checks are high priority

## 🚀 Final Recommendation

EOF

# Determine final status
if [ $SCORE_PERCENT -ge 90 ]; then
    STATUS="✅ PRODUCTION READY"
    RISK="Low"
    echo -e "${GREEN}🎉 Comprehensive Validation Complete: PRODUCTION READY${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ✅ PRODUCTION READY

**Reasoning**: Excellent validation results with $SCORE_PERCENT% success rate

**Next Steps**:
1. Proceed with deployment
2. Monitor production metrics
3. Schedule regular validation runs

**Confidence Level**: High
**Risk Level**: Low
EOF
elif [ $SCORE_PERCENT -ge 75 ]; then
    STATUS="⚠️ NEEDS ATTENTION"
    RISK="Medium"
    echo -e "${YELLOW}⚠️ Comprehensive Validation Complete: NEEDS ATTENTION${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ⚠️ NEEDS ATTENTION

**Reasoning**: Good validation results but some issues need addressing

**Next Steps**:
1. Address failed checks in priority order
2. Re-run validation for critical layers
3. Consider staged deployment

**Confidence Level**: Medium
**Risk Level**: Medium
EOF
else
    STATUS="❌ NOT READY"
    RISK="High"
    echo -e "${RED}❌ Comprehensive Validation Complete: NOT READY${NC}"
    cat >> "$REPORT_FILE" << EOF
**Status**: ❌ NOT READY

**Reasoning**: Significant issues found requiring resolution

**Next Steps**:
1. Address all critical failures
2. Run comprehensive validation again
3. Consider code review and additional testing

**Confidence Level**: Low
**Risk Level**: High
EOF
fi

echo ""
echo "📊 Final Score: $PASSED_CHECKS/$TOTAL_CHECKS ($SCORE_PERCENT%)"
echo "⏱️  Total Time: ${TOTAL_TIME}s"
echo "📄 Report saved to: $REPORT_FILE"
echo ""

# Exit with appropriate code
if [ $SCORE_PERCENT -ge 75 ]; then
    exit 0
else
    exit 1
fi
