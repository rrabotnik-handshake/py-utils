# ⏱️ Standardized Time Bands Reference

**Universal time bands used across all toolkit files**

This document defines the standard time bands used consistently throughout the Universal Refactor Validation Toolkit.

---

## 🎯 **Standard Time Bands**

### **⚡ Emergency/Urgent (30 seconds - 1 minute)**

- **Use for**: Hotfixes, critical bugs, immediate validation needs
- **Tools**: REFACTOR_VALIDATION_CHEATSHEET.md, 30-second prompts
- **Scope**: Essential checks only, copy-paste commands
- **Output**: Pass/fail status, immediate action items

### **🏃 Quick/Daily (5-10 minutes)**

- **Use for**: Daily development, code changes, PR validation
- **Tools**: REFACTOR_VALIDATION_CHEATSHEET.md + Mini Assessment, 2-minute prompts
- **Scope**: Code quality, basic tests, pattern checks
- **Output**: Mini assessment report, structured recommendations

### **🚶 Standard/Thorough (15-30 minutes)**

- **Use for**: Feature completion, pre-merge validation, pattern implementation
- **Tools**: DESIGN_PATTERN_VALIDATION.md, validate_patterns.py, comprehensive prompts
- **Scope**: Full pattern validation, integration tests, performance checks
- **Output**: Detailed pattern assessment, integration validation

### **🏃‍♂️ Comprehensive/Major (30-45 minutes)**

- **Use for**: Major refactors, production releases, architecture changes
- **Tools**: UNIVERSAL_REFACTOR_CHECKLIST.md, validate_full.sh
- **Scope**: All 6 validation layers, production readiness assessment
- **Output**: Comprehensive validation report, production readiness decision

---

## 📊 **Time Band Usage Matrix**

| **Scenario**          | **Time Band** | **Duration** | **Primary Tool**   | **Assessment Type**    |
| --------------------- | ------------- | ------------ | ------------------ | ---------------------- |
| 🔥 **Hotfix**         | Emergency     | 30s-1m       | Cheat Sheet        | Pass/Fail              |
| 📝 **Daily Dev**      | Quick         | 5-10m        | Cheat Sheet + Mini | Mini Report            |
| 🏗️ **Pattern Work**   | Standard      | 15-30m       | Pattern Guide      | Pattern Assessment     |
| 🔄 **Major Refactor** | Comprehensive | 30-45m       | Full Checklist     | Full Report            |
| 🚀 **Production**     | Comprehensive | 30-45m       | Full Checklist     | Production Readiness   |
| 🤖 **AI Quick**       | Quick         | 2-5m         | Quick Prompts      | Structured Response    |
| 🤖 **AI Deep**        | Comprehensive | 15-45m       | Full Prompts       | Comprehensive Response |

---

## 🎯 **File-Specific Time Standards**

### **REFACTOR_VALIDATION_CHEATSHEET.md**

- **Emergency**: 30 seconds (copy-paste commands)
- **Quick**: 5-10 minutes (commands + mini assessment)

### **QUICK_VALIDATION_PROMPTS.md**

- **30-second prompts**: Emergency validation
- **2-minute prompts**: Quick comprehensive check
- **5-minute prompts**: Standard validation with AI

### **DESIGN_PATTERN_VALIDATION.md**

- **Pattern check**: 15-30 minutes per pattern
- **Multi-pattern**: 30-45 minutes for architecture review

### **UNIVERSAL_REFACTOR_CHECKLIST.md**

- **Quick validation**: 5-10 minutes (Layer 1-2)
- **Standard validation**: 15-30 minutes (Layer 1-4)
- **Comprehensive**: 30-45 minutes (All 6 layers)

### **UNIVERSAL_AI_REFACTOR_PROMPT.md**

- **AI-guided validation**: 15-45 minutes depending on scope

### **validate_patterns.py**

- **Automated run**: 1-5 minutes
- **With manual review**: 10-15 minutes

### **validate_quick.sh**

- **Execution time**: 5-10 minutes
- **Includes**: Basic checks + mini assessment

### **validate_full.sh**

- **Execution time**: 30-45 minutes
- **Includes**: All layers + comprehensive assessment

---

## 🔄 **Consistency Rules**

### **Always Use These Exact Phrases**

- ✅ "30 seconds" (not "30s", "half a minute")
- ✅ "5-10 minutes" (not "5-10 min", "5 to 10 minutes")
- ✅ "15-30 minutes" (not "15-30 min", "quarter to half hour")
- ✅ "30-45 minutes" (not "30-45 min", "half to three-quarters hour")

### **Context-Specific Variations**

- **Tables**: Use "30s", "5-10m", "15-30m", "30-45m" for space
- **Headers**: Use full phrases "30 seconds", "5-10 minutes"
- **Body text**: Use full phrases consistently
- **Scripts**: Use abbreviated forms in comments/output

### **Never Use These Inconsistent Phrases**

- ❌ "A few minutes"
- ❌ "Quick check" (without time)
- ❌ "Comprehensive" (without time)
- ❌ "5 to 10 minutes"
- ❌ "Half an hour"
- ❌ "Around 30 minutes"

---

## 🎯 **Implementation Checklist**

When updating toolkit files, ensure:

- [ ] All time references use standard bands
- [ ] Table formats use abbreviated versions (30s, 5-10m, etc.)
- [ ] Headers and body text use full phrases
- [ ] No inconsistent time references remain
- [ ] Time bands match the actual tool capabilities
- [ ] Assessment types align with time investments

---

**This standard ensures users can quickly understand time commitments and choose appropriate validation approaches across all toolkit resources.**
