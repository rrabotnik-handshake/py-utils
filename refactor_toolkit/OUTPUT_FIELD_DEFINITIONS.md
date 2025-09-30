---
slug: output_field_definitions
version: 2.0.0
purpose: "Explicit field requirements for all assessment types"
inputs: ["assessment_type", "validation_results"]
outputs: ["structured_field_definitions", "validation_schemas"]
related: ["response_blueprints", "assessment_examples"]
---

# 📊 Output Field Definitions

**Explicit field requirements for all validation assessment types**

This document defines the exact fields that must be populated for each type of validation assessment, ensuring consistent and complete reporting across all tools and agents.

---

## 🎯 **Mini Assessment Fields**

### **Required Fields**

```json
{
  "status": "string", // ✅ Ready | ⚠️ Needs attention | ❌ Not ready
  "checks_passed": "integer", // Number of successful checks
  "total_checks": "integer", // Total number of checks performed
  "score_percent": "integer", // Percentage score (0-100)
  "issues_summary": "string", // Brief description of main issues
  "next_actions": "string" // Key actions needed
}
```

### **Optional Fields**

```json
{
  "validation_time": "string", // Time taken (e.g., "5m", "10m")
  "tech_stack": "string", // Technology stack validated
  "risk_level": "string", // Low | Medium | High
  "details": [
    {
      "check_name": "string",
      "status": "string", // ✅ | ❌ | ⚠️
      "result": "string"
    }
  ]
}
```

### **Validation Rules**

- `status` must be one of: "✅ Ready", "⚠️ Needs attention", "❌ Not ready"
- `checks_passed` must be ≤ `total_checks`
- `score_percent` must be calculated as `(checks_passed / total_checks) * 100`
- `issues_summary` required if `status` is not "✅ Ready"
- `next_actions` required if `status` is not "✅ Ready"

---

## 🏗️ **Comprehensive Assessment Fields**

### **Required Fields**

```json
{
  "metadata": {
    "date": "string", // ISO 8601 format
    "project_name": "string",
    "change_description": "string",
    "validation_time": "string", // Total time (e.g., "35m")
    "tech_stack": "string"
  },
  "validation_results": [
    {
      "layer_name": "string", // Code Quality, Unit Tests, Integration, etc.
      "status": "string", // ✅ | ❌ | ⚠️
      "results_summary": "string",
      "time_taken": "string", // e.g., "5m"
      "checks_passed": "integer",
      "total_checks": "integer"
    }
  ],
  "production_readiness": [
    {
      "criteria_name": "string", // Functionality, Reliability, Performance, etc.
      "status": "string", // ✅ | ❌ | ⚠️
      "evidence": "string"
    }
  ],
  "key_findings": {
    "strengths": ["string"], // Array of strength descriptions
    "issues_found": ["string"], // Array of issue descriptions
    "risks_identified": ["string"] // Array of risk descriptions
  },
  "final_recommendation": {
    "status": "string", // ✅ PRODUCTION READY | ⚠️ NEEDS ATTENTION | ❌ NOT READY
    "reasoning": "string",
    "next_steps": ["string"], // Array of prioritized actions
    "confidence_level": "string", // High | Medium | Low
    "risk_level": "string" // Low | Medium | High
  }
}
```

### **Validation Rules**

- `date` must be ISO 8601 format (YYYY-MM-DDTHH:mm:ssZ)
- `validation_results` must include at least: Code Quality, Unit Tests, Integration
- `production_readiness` must include: Functionality, Reliability, Performance, Maintainability, Security
- `final_recommendation.status` must match overall validation results
- `next_steps` must be prioritized (1., 2., 3., etc.)

---

## 🏗️ **Pattern Assessment Fields**

### **Required Fields**

```json
{
  "pattern_analysis": [
    {
      "pattern_name": "string", // Factory, Builder, Decorator, etc.
      "implementation_status": "string", // ✅ | ❌ | ⚠️
      "compliance_level": "string", // Excellent | Good | Poor
      "issues_summary": "string"
    }
  ],
  "detailed_findings": {
    "strengths": ["string"],
    "issues": ["string"],
    "recommendations": ["string"]
  },
  "pattern_readiness": {
    "status": "string", // ✅ WELL IMPLEMENTED | ⚠️ NEEDS IMPROVEMENT | ❌ POORLY IMPLEMENTED
    "architecture_quality": "string", // Excellent | Good | Needs Work | Poor
    "maintainability_impact": "string" // Positive | Neutral | Negative
  }
}
```

### **Validation Rules**

- `pattern_analysis` must include all patterns being validated
- `compliance_level` must align with `implementation_status`
- `pattern_readiness.status` must reflect overall pattern compliance
- `recommendations` required if any pattern has issues

---

## 💬 **PR Comment Fields**

### **Required Fields**

```json
{
  "overall_status": "string", // ✅ | ⚠️ | ❌
  "score_summary": "string", // "X/Y checks passed"
  "quick_summary": [
    {
      "category_name": "string", // Code Quality, Tests, Performance, etc.
      "status": "string", // ✅ | ❌ | ⚠️
      "brief_result": "string"
    }
  ],
  "action_required": "string", // What needs to be done
  "full_report_available": "boolean" // Whether detailed report is attached
}
```

### **Optional Fields**

```json
{
  "validation_time": "string",
  "tech_stack": "string",
  "change_type": "string", // hotfix, feature, refactor, etc.
  "full_report_content": "string" // Complete assessment if included
}
```

---

## 🤖 **AI Agent Response Fields**

### **Required Fields**

```json
{
  "response_type": "string", // quick_validation | comprehensive_validation | pattern_validation
  "tech_stack": "string",
  "validation_approach": "string", // Description of approach taken
  "commands_provided": ["string"], // Array of commands given
  "results": [
    {
      "check_name": "string",
      "status": "string", // ✅ | ❌ | ⚠️
      "result_description": "string"
    }
  ],
  "assessment": "string", // Overall assessment content
  "recommendation": "string", // Clear recommendation
  "next_steps": ["string"] // Actionable next steps
}
```

### **Optional Fields**

```json
{
  "time_estimate": "string",
  "risk_assessment": "string",
  "additional_resources": ["string"], // Links to relevant toolkit files
  "follow_up_questions": ["string"]
}
```

---

## 📋 **Field Validation Schema**

### **Status Field Values**

```json
{
  "valid_status_values": {
    "simple": ["✅", "❌", "⚠️"],
    "ready_status": ["✅ Ready", "⚠️ Needs attention", "❌ Not ready"],
    "production_status": [
      "✅ PRODUCTION READY",
      "⚠️ NEEDS ATTENTION",
      "❌ NOT READY"
    ],
    "pattern_status": [
      "✅ WELL IMPLEMENTED",
      "⚠️ NEEDS IMPROVEMENT",
      "❌ POORLY IMPLEMENTED"
    ]
  }
}
```

### **Quality Levels**

```json
{
  "quality_levels": {
    "compliance": ["Excellent", "Good", "Poor"],
    "architecture": ["Excellent", "Good", "Needs Work", "Poor"],
    "confidence": ["High", "Medium", "Low"],
    "risk": ["Low", "Medium", "High"],
    "impact": ["Positive", "Neutral", "Negative"]
  }
}
```

### **Required Field Combinations**

```json
{
  "field_dependencies": {
    "if_status_not_ready": ["issues_summary", "next_actions"],
    "if_comprehensive": [
      "validation_results",
      "production_readiness",
      "final_recommendation"
    ],
    "if_pattern_assessment": [
      "pattern_analysis",
      "detailed_findings",
      "pattern_readiness"
    ],
    "if_pr_comment": ["overall_status", "score_summary", "action_required"]
  }
}
```

---

## 🔧 **Implementation Guidelines**

### **For AI Agents**

1. **Validate required fields** before generating response
2. **Use exact field names** as specified in schemas
3. **Apply validation rules** to ensure data consistency
4. **Include optional fields** when relevant information is available
5. **Follow status value constraints** exactly as defined

### **For Automated Tools**

1. **Map tool outputs** to defined field structures
2. **Calculate derived fields** (e.g., score_percent) automatically
3. **Validate field types** and constraints before output
4. **Include metadata fields** for traceability
5. **Generate structured JSON** for programmatic consumption

### **For Human Users**

1. **Use field definitions** as checklists for manual assessments
2. **Ensure completeness** by checking required fields
3. **Follow validation rules** for consistency
4. **Include evidence** in appropriate fields
5. **Prioritize next steps** in order of importance

---

## 📊 **Example Field Population**

### **Mini Assessment Example**

```json
{
  "status": "⚠️ Needs attention",
  "checks_passed": 3,
  "total_checks": 4,
  "score_percent": 75,
  "issues_summary": "Type checking failures in user.py",
  "next_actions": "Fix mypy errors, then re-validate",
  "validation_time": "8m",
  "tech_stack": "Python",
  "risk_level": "Medium",
  "details": [
    {
      "check_name": "Linting",
      "status": "✅",
      "result": "Clean (ruff passed)"
    },
    {
      "check_name": "Type Checking",
      "status": "❌",
      "result": "3 mypy errors"
    },
    { "check_name": "Tests", "status": "✅", "result": "All tests passing" },
    {
      "check_name": "Imports",
      "status": "✅",
      "result": "Module loads correctly"
    }
  ]
}
```

### **Comprehensive Assessment Example**

```json
{
  "metadata": {
    "date": "2024-09-30T14:30:00Z",
    "project_name": "user-authentication-service",
    "change_description": "Refactored authentication middleware",
    "validation_time": "28m",
    "tech_stack": "Python"
  },
  "validation_results": [
    {
      "layer_name": "Code Quality",
      "status": "✅",
      "results_summary": "4/4 checks passed",
      "time_taken": "5m",
      "checks_passed": 4,
      "total_checks": 4
    }
  ],
  "production_readiness": [
    {
      "criteria_name": "Functionality",
      "status": "✅",
      "evidence": "All features work, comprehensive tests"
    }
  ],
  "key_findings": {
    "strengths": [
      "Excellent test coverage (94%)",
      "Clean Factory pattern implementation"
    ],
    "issues_found": [],
    "risks_identified": ["Low risk deployment"]
  },
  "final_recommendation": {
    "status": "✅ PRODUCTION READY",
    "reasoning": "Outstanding validation results with 100% layer success rate",
    "next_steps": ["Deploy to staging environment", "Run smoke tests"],
    "confidence_level": "High",
    "risk_level": "Low"
  }
}
```

---

**These field definitions ensure consistent, complete, and machine-readable validation outputs across all tools, agents, and scenarios.**
