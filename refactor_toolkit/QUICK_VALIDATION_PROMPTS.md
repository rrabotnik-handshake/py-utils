# ⚡ Quick Copy-Paste Validation Prompts

Ultra-concise prompts for immediate use. Just copy, fill in the blanks, and paste to your AI assistant.

## 🚀 **30-Second Prompts**

### **Basic Sanity Check**

```
I changed [WHAT] in my [TECH] [PROJECT_TYPE]. 5-minute validation:
- Run code quality checks
- Test basic functionality
- Check for obvious breaks
Commands for [TECH]?
```

### **Code Style Check**

```
[TECH] code style validation after changes:
- Check formatting, linting, unused imports
- Validate naming conventions
- Find type issues
Specific commands?
```

### **Test Everything**

```
Full validation for [TECH] after [CHANGE_TYPE]:
- Code quality + type checking
- Unit + integration tests
- Performance check
- Pattern validation
Step-by-step checklist?
```

---

## 🔍 **2-Minute Prompts**

### **Refactor Validation**

```
Refactored [COMPONENT] in [TECH]. Validate:
- No functionality broken
- Integration points work
- Performance unchanged
- Tests pass
Systematic approach?
```

### **Pattern Check**

```
Implemented [PATTERN] in [TECH]. Validate:
- Pattern correctly implemented
- Follows best practices
- No anti-patterns introduced
- Testable design
Specific checks?
```

### **Performance Validation**

```
Changed [COMPONENT] in [TECH]. Check performance:
- No regressions
- Memory usage OK
- Response times good
- Scalability intact
Tools and benchmarks?
```

---

## 🎯 **Technology-Specific (1-Minute)**

### **Python**

```
Python validation after changes:
- mypy, ruff, black, trunk check
- pytest with coverage
- Import and startup time
- Security scan
Commands?
```

### **JavaScript/Node.js**

```
JS/Node validation:
- eslint, prettier, tsc --noEmit
- npm test, npm audit
- Bundle size, performance
- E2E tests
Commands?
```

### **Java**

```
Java validation:
- mvn compile, checkstyle, spotbugs
- JUnit tests, JaCoCo coverage
- Performance, memory analysis
- Security scan
Commands?
```

### **Go**

```
Go validation:
- go vet, gofmt, golangci-lint
- go test -race -cover
- Benchmark tests
- Security check
Commands?
```

### **Rust**

```
Rust validation:
- cargo clippy, rustfmt --check
- cargo test, cargo bench
- Memory safety check
- Performance analysis
Commands?
```

---

## 🏗️ **Pattern-Specific (30-Second)**

### **Factory Pattern**

```
Validate Factory pattern in [TECH]:
- Returns interfaces not concrete classes
- Clients use factory not direct instantiation
- Extensible design
Issues to check?
```

### **Builder Pattern**

```
Validate Builder pattern in [TECH]:
- Method chaining works
- build() validates required fields
- Immutable or proper state handling
Issues to check?
```

### **Decorator Pattern**

```
Validate Decorator pattern in [TECH]:
- Implements same interface
- Delegates to wrapped component
- Stackable decorators
Issues to check?
```

---

## 🚨 **Critical Checks (1-Minute)**

### **Production Readiness**

```
Production deployment validation for [TECH]:
- All tests pass
- Performance meets SLA
- Security scans clean
- Monitoring configured
- Rollback plan ready
Go/no-go checklist?
```

### **Security Check**

```
Security validation for [TECH] changes:
- Input validation
- Authentication/authorization
- Data protection
- Vulnerability scan
Security checklist?
```

### **API Validation**

```
API changes validation:
- All endpoints work
- Backward compatibility
- Performance acceptable
- Security intact
- Documentation updated
API test strategy?
```

---

## 📊 **Specialized (45-Second)**

### **Database Changes**

```
Database integration validation:
- Migrations work
- Query performance good
- Data integrity maintained
- Connection pooling OK
Database checklist?
```

### **Large Codebase**

```
Validate changes in large [TECH] codebase:
- Efficient testing strategy
- Parallel execution
- Risk-based approach
- Fast feedback
Scalable validation plan?
```

### **Microservices**

```
Microservice changes validation:
- Service integration works
- API contracts maintained
- Performance acceptable
- Monitoring functional
Service validation approach?
```

---

## 🎨 **Custom Templates**

### **Fill-in-the-Blank Template**

```
Validate [CHANGE_TYPE] in my [TECH] [PROJECT_TYPE]:

**Context:**
- Technology: [Language/Framework]
- Project: [CLI/API/Web/Desktop]
- Changes: [What you modified]
- Risk: [Low/Medium/High]

**Validate:**
- [ ] Code quality and style
- [ ] Functionality works
- [ ] Tests pass
- [ ] Performance OK
- [ ] Integration points
- [ ] [Custom requirement]

**Time available:** [5/15/30 minutes]
**Focus areas:** [Specific concerns]

Provide step-by-step validation plan.
```

### **Problem-Specific Template**

```
I'm seeing [PROBLEM] after changing [WHAT] in [TECH].

**Issue:** [Describe the problem]
**Context:** [What you changed]
**Expected:** [What should happen]
**Actual:** [What's happening]

Help me:
1. Diagnose the root cause
2. Validate the fix
3. Prevent similar issues
4. Test thoroughly

Systematic debugging approach?
```

---

## 🔧 **Usage Tips**

### **How to Use These Prompts**

1. **Copy the relevant prompt**
2. **Fill in [BRACKETS] with your details**
3. **Paste to your AI assistant**
4. **Follow the provided guidance**
5. **Iterate with follow-up questions**

### **Customization Examples**

```
# Original
I changed [WHAT] in my [TECH] [PROJECT_TYPE].

# Customized
I changed the authentication middleware in my Python FastAPI web service.
```

### **Combining Prompts**

```
Start with: Basic Sanity Check
Then use: Pattern Check (if applicable)
Follow with: Performance Validation
End with: Production Readiness (if deploying)
```

### **Building Your Library**

- Save successful prompts for reuse
- Create project-specific variations
- Share effective prompts with team
- Iterate based on what works

---

## 📋 **Prompt Categories Quick Reference**

| **Need**                        | **Use This**           | **Time** |
| ------------------------------- | ---------------------- | -------- |
| Quick check after small changes | Basic Sanity Check     | 30 sec   |
| Code style and formatting       | Code Style Check       | 30 sec   |
| After major refactor            | Refactor Validation    | 2 min    |
| Design pattern implementation   | Pattern Check          | 2 min    |
| Performance concerns            | Performance Validation | 2 min    |
| Before production deploy        | Production Readiness   | 1 min    |
| Security validation             | Security Check         | 1 min    |
| API changes                     | API Validation         | 45 sec   |
| Database modifications          | Database Changes       | 45 sec   |
| Large codebase changes          | Large Codebase         | 45 sec   |
| Language-specific issues        | Technology-Specific    | 1 min    |

---

**Pro Tip**: Start with the shortest relevant prompt, then ask follow-up questions to dive deeper into specific areas that need attention!
