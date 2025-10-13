#!/bin/bash

# Enhanced validation script with machine-readable output
# References validation_schema.yaml for consistency

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/validation_schema.yaml"

# Default output mode
OUTPUT_MODE="human"
PROFILE="quick"
EXIT_ON_FAILURE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --json)
            OUTPUT_MODE="json"
            shift
            ;;
        --machine)
            OUTPUT_MODE="machine"
            shift
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --exit-on-failure)
            EXIT_ON_FAILURE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--json|--machine] [--profile PROFILE] [--exit-on-failure]"
            echo "Profiles: emergency, quick, standard, comprehensive"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 5
            ;;
    esac
done

# Initialize results
declare -A LAYER_RESULTS
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
START_TIME=$(date +%s)
TIMESTAMP=$(date -Iseconds)
PROJECT_NAME=$(basename "$(pwd)")

# Output functions
log_human() {
    if [[ "$OUTPUT_MODE" == "human" ]]; then
        echo "$@"
    fi
}

# Execute command with result tracking
execute_check() {
    local layer="$1"
    local cmd="$2"
    local description="$3"

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    local start_time=$(date +%s)

    log_human "  Running: $description"

    if eval "$cmd" >/dev/null 2>&1; then
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        local status="PASS"
        local exit_code=0
    else
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        local status="FAIL"
        local exit_code=$?

        if [[ "$EXIT_ON_FAILURE" == "true" ]]; then
            log_human "❌ Critical failure in $layer: $description"
            exit 2
        fi
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    LAYER_RESULTS["$layer"]="${LAYER_RESULTS["$layer"]}${status}:${description}:${duration}:${exit_code};"

    log_human "    → $status (${duration}s)"
}

# Detect technology stack
detect_tech() {
    local tech=""

    if [[ -f "pyproject.toml" || -f "requirements.txt" || -f "setup.py" ]]; then
        tech="python"
    elif [[ -f "package.json" ]]; then
        tech="javascript"
    elif [[ -f "go.mod" ]]; then
        tech="go"
    elif [[ -f "Cargo.toml" ]]; then
        tech="rust"
    elif [[ -f "pom.xml" || -f "build.gradle" ]]; then
        tech="java"
    else
        tech="generic"
    fi

    echo "$tech"
}

# Main validation logic
main() {
    local tech=$(detect_tech)

    log_human "🚀 Quick Validation ($PROFILE profile)"
    log_human "=================================="
    log_human "Technology: $tech"
    log_human "Project: $PROJECT_NAME"
    log_human ""

    # Layer 1: Code Quality
    log_human "📝 Code Quality..."
    case "$tech" in
        python)
            if command -v ruff &> /dev/null; then
                execute_check "code_quality" "ruff check ." "Ruff linting"
            fi
            if command -v mypy &> /dev/null; then
                execute_check "code_quality" "mypy ." "MyPy type checking"
            fi
            if command -v black &> /dev/null; then
                execute_check "code_quality" "black --check ." "Black formatting"
            fi
            ;;
        javascript)
            if command -v npx &> /dev/null; then
                execute_check "code_quality" "npx eslint ." "ESLint"
                execute_check "code_quality" "npx tsc --noEmit" "TypeScript compilation"
            fi
            ;;
        go)
            execute_check "code_quality" "gofmt -l . | wc -l | grep '^0$'" "Go formatting"
            execute_check "code_quality" "go vet ./..." "Go vet"
            ;;
        rust)
            execute_check "code_quality" "cargo fmt -- --check" "Rust formatting"
            execute_check "code_quality" "cargo clippy -- -D warnings" "Clippy linting"
            ;;
        java)
            if command -v mvn &> /dev/null; then
                execute_check "code_quality" "mvn checkstyle:check -q" "Checkstyle"
            fi
            ;;
    esac

    # Layer 2: Unit Tests
    log_human ""
    log_human "🧪 Unit Tests..."
    case "$tech" in
        python)
            if command -v pytest &> /dev/null; then
                execute_check "unit_tests" "pytest -v" "Pytest execution"
            fi
            ;;
        javascript)
            if [[ -f "package.json" ]]; then
                execute_check "unit_tests" "npm test" "NPM test suite"
            fi
            ;;
        go)
            execute_check "unit_tests" "go test ./..." "Go test suite"
            ;;
        rust)
            execute_check "unit_tests" "cargo test" "Cargo test suite"
            ;;
        java)
            if command -v mvn &> /dev/null; then
                execute_check "unit_tests" "mvn test -q" "Maven test suite"
            fi
            ;;
    esac

    # Layer 3: Integration
    log_human ""
    log_human "🔗 Integration..."
    case "$tech" in
        python)
            execute_check "integration" "python -c 'import sys; sys.path.insert(0, \".\"); import src'" "Import validation"
            if command -v pip &> /dev/null; then
                execute_check "integration" "pip check" "Dependency check"
            fi
            ;;
        javascript)
            if [[ -f "package.json" ]]; then
                execute_check "integration" "npm run build" "Build verification"
            fi
            ;;
        go)
            execute_check "integration" "go build ./..." "Build verification"
            ;;
        rust)
            execute_check "integration" "cargo build" "Build verification"
            ;;
        java)
            if command -v mvn &> /dev/null; then
                execute_check "integration" "mvn compile -q" "Compilation check"
            fi
            ;;
    esac

    # Generate final output
    END_TIME=$(date +%s)
    TOTAL_DURATION=$((END_TIME - START_TIME))

    case "$OUTPUT_MODE" in
        json)
            generate_json_output
            ;;
        machine)
            generate_machine_output
            ;;
        *)
            generate_human_output
            ;;
    esac

    # Exit with appropriate code
    if [[ $FAILED_CHECKS -eq 0 ]]; then
        exit 0  # Success
    elif [[ $FAILED_CHECKS -le 1 ]]; then
        exit 1  # Warnings only
    else
        exit 2  # Failures found
    fi
}

generate_json_output() {
    cat << EOF
{
  "metadata": {
    "timestamp": "$TIMESTAMP",
    "project_name": "$PROJECT_NAME",
    "profile": "$PROFILE",
    "validation_time": "${TOTAL_DURATION}s",
    "tech_stack": "$(detect_tech)"
  },
  "summary": {
    "total_checks": $TOTAL_CHECKS,
    "passed_checks": $PASSED_CHECKS,
    "failed_checks": $FAILED_CHECKS,
    "overall_result": "$([ $FAILED_CHECKS -eq 0 ] && echo "all_passed" || echo "some_failed")"
  },
  "layer_results": [
$(for layer in "${!LAYER_RESULTS[@]}"; do
    echo "    {"
    echo "      \"layer_name\": \"$layer\","
    echo "      \"results\": ["
    IFS=';' read -ra RESULTS <<< "${LAYER_RESULTS[$layer]}"
    for i in "${!RESULTS[@]}"; do
        if [[ -n "${RESULTS[$i]}" ]]; then
            IFS=':' read -ra RESULT <<< "${RESULTS[$i]}"
            echo "        {"
            echo "          \"status\": \"${RESULT[0]}\","
            echo "          \"description\": \"${RESULT[1]}\","
            echo "          \"duration\": ${RESULT[2]},"
            echo "          \"exit_code\": ${RESULT[3]}"
            echo -n "        }"
            [[ $i -lt $((${#RESULTS[@]} - 2)) ]] && echo ","
            echo ""
        fi
    done
    echo "      ]"
    echo -n "    }"
    [[ "$layer" != "${!LAYER_RESULTS[@]: -1}" ]] && echo ","
    echo ""
done)
  ],
  "final_recommendation": {
    "status": "$([ $FAILED_CHECKS -eq 0 ] && echo "✅ PRODUCTION READY" || echo "⚠️ NEEDS ATTENTION")",
    "next_actions": "$([ $FAILED_CHECKS -gt 0 ] && echo "Review failed checks and address issues" || echo "All checks passed - ready for deployment")"
  }
}
EOF
}

generate_machine_output() {
    echo "VALIDATION_RESULT:$([ $FAILED_CHECKS -eq 0 ] && echo "PASS" || echo "FAIL")"
    echo "TOTAL_CHECKS:$TOTAL_CHECKS"
    echo "PASSED_CHECKS:$PASSED_CHECKS"
    echo "FAILED_CHECKS:$FAILED_CHECKS"
    echo "DURATION:${TOTAL_DURATION}s"
    echo "PROJECT:$PROJECT_NAME"
    echo "TIMESTAMP:$TIMESTAMP"

    for layer in "${!LAYER_RESULTS[@]}"; do
        echo "LAYER:$layer:${LAYER_RESULTS[$layer]}"
    done
}

generate_human_output() {
    log_human ""
    log_human "📊 Validation Summary"
    log_human "===================="
    log_human "Total checks: $TOTAL_CHECKS"
    log_human "Passed: $PASSED_CHECKS"
    log_human "Failed: $FAILED_CHECKS"
    log_human "Duration: ${TOTAL_DURATION}s"
    log_human ""

    if [[ $FAILED_CHECKS -eq 0 ]]; then
        log_human "✅ All checks passed - ready for deployment!"
    else
        log_human "⚠️ $FAILED_CHECKS check(s) failed - review and address issues"
        log_human ""
        log_human "Failed checks:"
        for layer in "${!LAYER_RESULTS[@]}"; do
            IFS=';' read -ra RESULTS <<< "${LAYER_RESULTS[$layer]}"
            for result in "${RESULTS[@]}"; do
                if [[ -n "$result" ]]; then
                    IFS=':' read -ra RESULT <<< "$result"
                    if [[ "${RESULT[0]}" == "FAIL" ]]; then
                        log_human "  ❌ $layer: ${RESULT[1]}"
                    fi
                fi
            done
        done
    fi
}

# Run main function
main "$@"
