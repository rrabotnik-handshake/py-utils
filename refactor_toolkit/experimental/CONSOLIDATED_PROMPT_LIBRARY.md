---
slug: consolidated_prompt_library
version: 3.0.0
time_bands: ["30s", "5-10m", "15-30m", "30-45m"]
scenarios:
  [
    "emergency",
    "daily",
    "major_refactor",
    "pre_production",
    "architecture_review",
    "pattern_focused",
    "performance_focused",
  ]
inputs:
  [
    "tech_stack",
    "change_complexity",
    "risk_level",
    "time_available",
    "validation_focus",
  ]
outputs:
  [
    "mini_report",
    "comprehensive_assessment",
    "pattern_report",
    "performance_analysis",
    "production_readiness",
  ]
related:
  [
    "validation_schema.yaml",
    "context_binding_schema.json",
    "ANTI_PATTERN_CATALOG.md",
  ]
auto_detectable: true
---

# 🚀 **Consolidated AI Prompt Library**

**Single source of truth for all validation prompts - no more scanning multiple files!**

Choose your prompt based on **time available** and **validation focus**:

## 📋 **Quick Selection Guide**

| **Time** | **Focus**      | **Prompt ID** | **What You Get**                     |
| -------- | -------------- | ------------- | ------------------------------------ |
| **30s**  | Emergency      | `P001`        | Critical checks only                 |
| **5m**   | Daily Dev      | `P002`        | Standard validation                  |
| **10m**  | Code Quality   | `P003`        | Linting + tests + patterns           |
| **15m**  | Architecture   | `P004`        | Pattern compliance + anti-patterns   |
| **30m**  | Pre-Production | `P005`        | Full validation + migration analysis |
| **45m**  | Performance    | `P006`        | Performance + security + complexity  |

---

## 🔥 **Emergency Prompts (30s-1m)**

### **P001: Critical Hotfix Validation**

```
Emergency validation for [TECH_STACK] after [CHANGE_TYPE]:
- Run only critical checks (linting, compilation, import validation)
- Exit on first failure
- Generate mini assessment with pass/fail status
- Include immediate next actions if failed
- Use validation_schema.yaml emergency profile
- Output: 30-second assessment with clear go/no-go decision
```

**Usage**: `./validate_quick_enhanced.sh --profile emergency --exit-on-failure --json`

---

## ⚡ **Quick Daily Prompts (5-10m)**

### **P002: Standard Daily Validation**

```
Quick validation for [TECH_STACK] after [CHANGE_TYPE]:
- Code quality (linting, type checking, formatting)
- Unit tests with coverage check
- Integration/build verification
- Basic pattern validation
- Generate structured assessment using context_binding_schema.json
- Reference validation_schema.yaml quick profile
- Output: Mini assessment + actionable recommendations
```

**Usage**: `./validate_quick_enhanced.sh --profile quick --json`

### **P003: Code Quality Focus**

```
Code quality validation for [TECH_STACK]:
- Comprehensive linting with multiple tools
- Type checking with detailed error analysis
- Code formatting and style compliance
- Import dependency validation
- Anti-pattern detection (god objects, spaghetti code)
- Reference ANTI_PATTERN_CATALOG.md for remediation
- Output: Quality-focused assessment with specific fixes
```

**Usage**: `./validate_quick_enhanced.sh --profile quick --json | jq '.layer_results[] | select(.layer_name == "code_quality")'`

---

## 🏗️ **Architecture & Pattern Prompts (15-30m)**

### **P004: Pattern Compliance Validation**

```
Design pattern validation for [TECH_STACK]:
- Run validate_patterns.py with comprehensive checks
- Detect anti-patterns using ANTI_PATTERN_CATALOG.md
- Validate architectural compliance
- Check for circular dependencies and coupling issues
- Analyze complexity metrics (cyclomatic, maintainability)
- Generate pattern-focused assessment
- Include specific remediation strategies
- Output: Pattern assessment with architectural recommendations
```

**Usage**: `python validate_patterns.py . --auto-detect --include-anti-patterns --json`

### **P005: Pre-Production Validation**

```
Full pre-production validation for [TECH_STACK]:
- All validation layers from validation_schema.yaml standard profile
- Code quality + unit tests + integration + patterns + security
- Migration analysis if comparing schemas/data
- Performance regression detection
- Security vulnerability scanning
- Generate comprehensive assessment using RESPONSE_BLUEPRINTS.md
- Include production readiness decision
- Output: Complete production readiness report
```

**Usage**: `./validate_full.sh --profile comprehensive --json`

---

## 🚀 **Comprehensive Prompts (30-45m)**

### **P006: Performance & Security Deep Dive**

```
Performance and security validation for [TECH_STACK]:
- All standard validation layers
- Performance profiling and complexity analysis
- Security vulnerability scanning (Bandit, npm audit, etc.)
- Load testing and memory analysis
- Dependency security audit
- Code complexity and maintainability metrics
- Generate performance-focused assessment
- Include optimization recommendations
- Output: Performance + security comprehensive report
```

**Usage**: `./validate_full.sh --profile comprehensive --include-performance --include-security --json`

### **P007: Migration Impact Analysis**

```
Schema/data migration validation:
- Compare source and target schemas using schema-diff
- Generate migration analysis report
- Identify breaking changes and compatibility issues
- Assess data transformation requirements
- Validate backward compatibility
- Include migration strategy recommendations
- Reference migration patterns and best practices
- Output: Migration impact analysis with action plan
```

**Usage**: `schema-diff [SOURCE] [TARGET] --output --migration-analysis`

---

## 🎯 **Specialized Prompts**

### **P008: CI/CD Pipeline Validation**

```
CI/CD pipeline validation for [TECH_STACK]:
- Machine-readable output for pipeline integration
- Layer-by-layer exit codes for partial results
- JSON output compatible with CI/CD tools
- Failure categorization (critical vs warnings)
- Integration with existing pipeline tools
- Reference CI_CD_INTEGRATION.md for setup
- Output: Machine-readable validation results
```

**Usage**: `./validate_quick_enhanced.sh --machine --profile [PROFILE]`

### **P009: Code Review Assistant**

```
Code review validation for PR/MR:
- Focus on changed files and their dependencies
- Pattern compliance for new code
- Test coverage for modified functions
- Anti-pattern detection in changes
- Generate PR comment using RESPONSE_BLUEPRINTS.md
- Include specific line-level recommendations
- Output: PR comment with validation results
```

**Usage**: `git diff --name-only | xargs python validate_patterns.py --files`

### **P010: Refactor Safety Check**

```
Post-refactor safety validation:
- Comprehensive validation using UNIVERSAL_REFACTOR_CHECKLIST.md
- Before/after comparison if applicable
- Regression testing focus
- Pattern compliance verification
- Performance impact analysis
- Generate refactor assessment
- Include rollback recommendations if needed
- Output: Refactor safety assessment
```

**Usage**: Follow `UNIVERSAL_REFACTOR_CHECKLIST.md` with AI assistance

---

## 🤖 **AI Agent Integration**

### **Agent Decision Tree**

```yaml
user_intent:
  emergency_fix:
    time_available: "< 2m"
    recommended_prompt: "P001"
    fallback_action: "Run critical checks only"

  daily_development:
    time_available: "5-10m"
    recommended_prompt: "P002"
    fallback_action: "Standard validation"

  pre_commit_hook:
    time_available: "2-5m"
    recommended_prompt: "P003"
    fallback_action: "Code quality focus"

  architecture_review:
    time_available: "15-30m"
    recommended_prompt: "P004"
    fallback_action: "Pattern validation"

  production_deployment:
    time_available: "30-45m"
    recommended_prompt: "P005"
    fallback_action: "Full validation"

  performance_analysis:
    time_available: "30-60m"
    recommended_prompt: "P006"
    fallback_action: "Performance deep dive"
```

### **Response Templates**

#### **Quick Response (P001-P003)**

```markdown
## 🚀 **Quick Validation Results**

**Status**: {{status}}
**Checks**: {{checks_passed}}/{{total_checks}} passed
**Duration**: {{validation_time}}

### **Summary**

{{issues_summary}}

### **Next Actions**

{{next_actions}}

**Generated using**: {{prompt_id}} | **Project**: {{project_name}}
```

#### **Comprehensive Response (P004-P007)**

```markdown
## 📊 **Comprehensive Validation Assessment**

**Date**: {{date}}
**Project**: {{project_name}}
**Validation Time**: {{validation_time}}
**Tech Stack**: {{tech_stack}}

### **🎯 Validation Results**

{{validation_results}}

### **🏗️ Pattern Analysis**

{{pattern_analysis}}

### **⚠️ Critical Issues**

{{critical_issues}}

### **✅ Final Recommendation**

{{final_recommendation}}

**Generated using**: {{prompt_id}} | **Schema**: validation_schema.yaml
```

---

## 🔧 **Integration Examples**

### **With Existing Scripts**

```bash
# Quick validation with JSON output
./validate_quick_enhanced.sh --json | jq -r '.final_recommendation.status'

# Pattern validation with anti-pattern detection
python validate_patterns.py . --auto-detect --include-anti-patterns

# Full validation with comprehensive report
./validate_full.sh --profile comprehensive --output
```

### **With CI/CD Pipelines**

```yaml
# GitHub Actions example
- name: Validate Code
  run: |
    ./validate_quick_enhanced.sh --machine --profile quick
    echo "VALIDATION_STATUS=$?" >> $GITHUB_ENV

- name: Generate Report
  if: env.VALIDATION_STATUS != '0'
  run: |
    ./validate_quick_enhanced.sh --json > validation_report.json
```

### **With AI Assistants**

```
# For AI agents - use this format:
"Run validation using prompt {{prompt_id}} for {{tech_stack}} project with {{time_available}} time budget. Generate {{output_type}} assessment."

# Example:
"Run validation using prompt P005 for python project with 30m time budget. Generate comprehensive_assessment."
```

---

## 📚 **Reference Integration**

All prompts reference these core resources:

- **`validation_schema.yaml`**: Central validation definitions
- **`context_binding_schema.json`**: Output format mappings
- **`ANTI_PATTERN_CATALOG.md`**: Anti-pattern detection and remediation
- **`RESPONSE_BLUEPRINTS.md`**: Template structures
- **`validation_flows.yaml`**: Declarative workflow definitions

---

## 🎯 **Success Criteria**

### **Emergency (P001)**

- ✅ Critical checks pass
- ✅ Clear go/no-go decision
- ✅ < 1 minute execution

### **Daily (P002-P003)**

- ✅ Code quality + tests pass
- ✅ Actionable recommendations
- ✅ < 10 minute execution

### **Architecture (P004)**

- ✅ Pattern compliance verified
- ✅ Anti-patterns identified
- ✅ Remediation strategies provided

### **Production (P005-P007)**

- ✅ All validation layers pass
- ✅ Production readiness confirmed
- ✅ Migration analysis complete

---

## 💡 **Pro Tips**

1. **Start with the right prompt**: Use the Quick Selection Guide above
2. **Chain prompts**: Use P002 → P004 → P005 for progressive validation
3. **Customize for your stack**: All prompts support tech-specific variations
4. **Integrate with tools**: Use `--json` and `--machine` flags for automation
5. **Reference schemas**: Always check `validation_schema.yaml` for latest definitions

**🤖 For AI Agents**: Use `AGENT_NAVIGATION_GUIDE.md` for detailed integration instructions.
