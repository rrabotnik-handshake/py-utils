---
slug: response_blueprints
version: 2.0.0
purpose: "Machine-fillable templates for AI agents and automated tools"
inputs: ["validation_results", "assessment_data", "context_variables"]
outputs: ["structured_reports", "formatted_responses"]
related: ["assessment_examples", "agent_navigation_guide"]
---

# 🤖 Response Blueprints

**Machine-fillable templates for consistent, structured validation responses**

These templates provide standardized slots that AI agents and automated tools can populate to generate consistent validation reports and responses.

---

## 📊 **Mini Validation Report Blueprint**

```markdown
## 📊 Mini Validation Report

**Status**: {{status}}
**Checks**: {{checks_passed}}/{{total_checks}} passed
**Issues**: {{issues_summary}}
**Next**: {{next_actions}}

**Details**:
{{#each_check}}

- {{status_icon}} {{check_name}}: {{result_summary}}
  {{/each_check}}
```

### **Template Variables**

- `{{status}}`: ✅ Ready / ⚠️ Needs attention / ❌ Not ready
- `{{checks_passed}}`: Number of successful checks
- `{{total_checks}}`: Total number of checks performed
- `{{issues_summary}}`: Brief description of main issues
- `{{next_actions}}`: Key actions needed
- `{{each_check}}`: Loop through individual check results
  - `{{status_icon}}`: ✅/❌/⚠️
  - `{{check_name}}`: Name of the check
  - `{{result_summary}}`: Brief result description

---

## 🏗️ **Comprehensive Assessment Blueprint**

```markdown
## 📊 Comprehensive Validation Assessment

**Date**: {{current_date}}
**Project**: {{project_name}}
**Changes**: {{change_description}}
**Validation Time**: {{total_time}}

### ✅ Validation Results

| **Layer** | **Status** | **Results** | **Time** |
| --------- | ---------- | ----------- | -------- |

{{#each_layer}}
| {{layer_name}} | {{status_icon}} | {{results_summary}} | {{time_taken}} |
{{/each_layer}}

**Total Validation Time**: {{total_time}}

### 🎯 Production Readiness

| **Criteria** | **Status** | **Evidence** |
| ------------ | ---------- | ------------ |

{{#each_criteria}}
| {{criteria_name}} | {{status_icon}} | {{evidence_summary}} |
{{/each_criteria}}

### 💡 Key Findings

**Strengths**:
{{#each_strength}}

- {{strength_description}}
  {{/each_strength}}

**Issues Found**:
{{#each_issue}}

- {{issue_description}}
  {{/each_issue}}

**Risks Identified**:
{{#each_risk}}

- {{risk_description}}
  {{/each_risk}}

### 🚀 Final Recommendation

**Status**: {{final_status}}

**Reasoning**: {{reasoning}}

**Next Steps**:
{{#each_step}}
{{step_priority}}. {{step_description}}
{{/each_step}}

**Confidence Level**: {{confidence_level}}
**Risk Level**: {{risk_level}}
```

### **Template Variables**

- `{{current_date}}`: Current date/time
- `{{project_name}}`: Name of the project
- `{{change_description}}`: Description of changes made
- `{{total_time}}`: Total validation time
- `{{each_layer}}`: Loop through validation layers
- `{{each_criteria}}`: Loop through production readiness criteria
- `{{each_strength}}`: Loop through identified strengths
- `{{each_issue}}`: Loop through issues found
- `{{each_risk}}`: Loop through risks identified
- `{{final_status}}`: ✅ PRODUCTION READY / ⚠️ NEEDS ATTENTION / ❌ NOT READY
- `{{reasoning}}`: Clear explanation of decision
- `{{each_step}}`: Loop through next steps
- `{{confidence_level}}`: High / Medium / Low
- `{{risk_level}}`: Low / Medium / High

---

## 🏗️ **Pattern Assessment Blueprint**

```markdown
## 📊 Design Pattern Validation Assessment

### 🏗️ Pattern Analysis

| **Pattern** | **Implementation** | **Compliance** | **Issues** |
| ----------- | ------------------ | -------------- | ---------- |

{{#each_pattern}}
| {{pattern_name}} | {{implementation_status}} | {{compliance_level}} | {{issues_summary}} |
{{/each_pattern}}

### 🔍 Detailed Findings

**Strengths**:
{{#each_strength}}

- {{strength_description}}
  {{/each_strength}}

**Issues**:
{{#each_issue}}

- {{issue_description}}
  {{/each_issue}}

**Recommendations**:
{{#each_recommendation}}

- {{recommendation_description}}
  {{/each_recommendation}}

### 🎯 Pattern Readiness

**Status**: {{pattern_status}}
**Architecture Quality**: {{architecture_quality}}
**Maintainability Impact**: {{maintainability_impact}}
```

### **Template Variables**

- `{{each_pattern}}`: Loop through analyzed patterns
- `{{pattern_name}}`: Name of the pattern
- `{{implementation_status}}`: ✅/❌/⚠️
- `{{compliance_level}}`: Excellent / Good / Poor
- `{{issues_summary}}`: Brief issue description
- `{{pattern_status}}`: ✅ WELL IMPLEMENTED / ⚠️ NEEDS IMPROVEMENT / ❌ POORLY IMPLEMENTED
- `{{architecture_quality}}`: Excellent / Good / Needs Work / Poor
- `{{maintainability_impact}}`: Positive / Neutral / Negative

---

## 💬 **PR Comment Blueprint**

```markdown
## 🔍 Validation Results

**Overall**: {{overall_status}} | **Score**: {{score_summary}}

**Quick Summary**:
{{#each_category}}

- {{status_icon}} **{{category_name}}**: {{brief_result}}
  {{/each_category}}

**Action Required**: {{action_required}}

<details>
<summary>📊 Full Validation Report</summary>

{{full_report_content}}

</details>
```

### **Template Variables**

- `{{overall_status}}`: ✅/⚠️/❌
- `{{score_summary}}`: X/Y checks passed
- `{{each_category}}`: Loop through validation categories
- `{{action_required}}`: What needs to be done
- `{{full_report_content}}`: Complete assessment report

---

## 🤖 **AI Agent Response Blueprint**

````markdown
I'll help you validate your {{tech_stack}} changes using our systematic approach:

## 🚀 **{{validation_type}} Validation**

{{#if_quick}}
**Quick Check (5-10 minutes)**:
{{/if_quick}}
{{#if_comprehensive}}
**Comprehensive Analysis (30-45 minutes)**:
{{/if_comprehensive}}

### **{{layer_name}}**

```bash
{{commands}}
```
````

{{#each_result}}
{{status_icon}} **{{check_name}}**: {{result_description}}
{{/each_result}}

## 📊 **Assessment**

{{assessment_content}}

**Recommendation**: {{recommendation}}
**Next Steps**: {{next_steps}}

````

### **Template Variables**
- `{{tech_stack}}`: User's technology stack
- `{{validation_type}}`: Quick / Comprehensive / Pattern-Focused
- `{{if_quick}}` / `{{if_comprehensive}}`: Conditional sections
- `{{layer_name}}`: Current validation layer
- `{{commands}}`: Specific commands to run
- `{{recommendation}}`: Clear recommendation
- `{{next_steps}}`: Actionable next steps

---

## 🔧 **Command Execution Blueprint**

```yaml
flow: {{flow_name}}
tech_stack: {{tech_stack}}
steps:
{{#each_step}}
  - name: {{step_name}}
    cmd: "{{command}}"
    expect_regex: "{{success_pattern}}"
    timeout_s: {{timeout_seconds}}
    on_fail_tip: "{{failure_guidance}}"
{{/each_step}}
````

### **Template Variables**

- `{{flow_name}}`: Name of the validation flow
- `{{tech_stack}}`: Technology stack being validated
- `{{each_step}}`: Loop through validation steps
- `{{step_name}}`: Name of the step
- `{{command}}`: Command to execute
- `{{success_pattern}}`: Regex pattern for success
- `{{timeout_seconds}}`: Timeout in seconds
- `{{failure_guidance}}`: What to do if step fails

---

## 📋 **Usage Examples**

### **Example 1: Populated Mini Report**

```markdown
## 📊 Mini Validation Report

**Status**: ⚠️ Needs attention
**Checks**: 3/4 passed
**Issues**: Type checking failures
**Next**: Fix mypy errors, then re-validate

**Details**:

- ✅ Linting: Clean (ruff passed)
- ❌ Types: 3 mypy errors in user.py
- ✅ Tests: All tests passing
- ✅ Imports: Module loads correctly
```

### **Example 2: AI Agent Response**

````markdown
I'll help you validate your Python changes using our systematic approach:

## 🚀 **Quick Validation**

**Quick Check (5-10 minutes)**:

### **Code Quality**

```bash
ruff check && mypy . && trunk check .
```
````

✅ **Linting**: No issues found
❌ **Type Checking**: 3 errors in user.py
✅ **Formatting**: Code properly formatted

## 📊 **Assessment**

Your code quality is good overall, but there are type checking issues that need attention.

**Recommendation**: Fix the mypy errors before proceeding
**Next Steps**: Run `mypy user.py --show-error-codes` to see specific issues

```

---

## 🎯 **Integration Guidelines**

### **For AI Agents**
1. **Parse context** to determine appropriate blueprint
2. **Fill template variables** with actual validation results
3. **Apply conditional logic** for different scenarios
4. **Return formatted response** using the blueprint structure

### **For Automated Tools**
1. **Execute validation steps** and collect results
2. **Map results to template variables**
3. **Generate structured output** using blueprints
4. **Save or display** formatted reports

### **For Human Users**
1. **Use as reference** for consistent reporting
2. **Adapt templates** for specific project needs
3. **Copy and modify** for custom scenarios
4. **Integrate with CI/CD** for automated reporting

---

**These blueprints ensure consistent, professional validation responses across all tools, agents, and scenarios while maintaining flexibility for customization.**
```
