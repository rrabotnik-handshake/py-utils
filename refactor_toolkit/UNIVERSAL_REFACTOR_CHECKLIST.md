# 🔍 Universal Post-Refactor Sanity Checklist

Use this checklist after any significant code refactor, architectural changes, or major modifications to ensure nothing is broken. Adapt the specific commands to your technology stack.

## 🎯 Quick Validation (5-10 minutes)

### 1. **Code Quality & Standards**

```bash
# Linting (adapt to your language)
# Python: flake8, pylint, ruff, trunk check
# JavaScript: eslint
# Java: checkstyle, spotbugs
# Go: golint, go vet
# Rust: clippy

# Type checking (if applicable)
# Python: mypy
# TypeScript: tsc --noEmit
# Java: built into compiler
# Go: built into compiler

# Formatting check
# Python: black --check, trunk fmt
# JavaScript: prettier --check
# Go: gofmt -d
# Rust: rustfmt --check

# Code style validation (CRITICAL - often missed!)
# Python: ruff check, trunk check
# JavaScript: eslint --fix-dry-run
# Java: checkstyle, spotbugs
# Go: golangci-lint run
# Rust: clippy --all-targets
```

### 2. **Basic Functionality Smoke Test**

```bash
# Test main entry points
your-app --help                    # CLI tools
curl http://localhost:8080/health  # Web services
your-lib.main_function()           # Libraries

# Test core workflows
your-app basic-command
your-service start && curl /api/status
```

## 🧪 Comprehensive Testing (15-30 minutes)

### 3. **Unit Tests - Component Level**

```bash
# Run all unit tests
# Python: pytest tests/
# JavaScript: npm test
# Java: mvn test
# Go: go test ./...
# Rust: cargo test

# With coverage (adapt command)
# Python: pytest --cov=src/
# JavaScript: npm run test:coverage
# Java: mvn test jacoco:report
# Go: go test -cover ./...
# Rust: cargo tarpaulin
```

### 4. **Integration/Functional Tests - End-to-End Workflows**

```bash
# Test complete user journeys
# Web apps: Selenium, Cypress, Playwright
# APIs: Postman, REST Assured, supertest
# CLI tools: End-to-end command tests
# Libraries: Integration test suites

# Test critical user workflows manually
your-app complete-workflow-1
your-app complete-workflow-2
```

### 5. **Component Integration Validation**

```bash
# Test parameter passing between modules
# Test file I/O and external integrations
# Test error handling and edge cases
# Test configuration loading
# Test database connections (if applicable)
```

### 6. **Design Pattern Validation** (if applicable)

```bash
# Validate common patterns (see DESIGN_PATTERN_VALIDATION.md)
# Factory: Check interface returns, client usage
# Builder: Verify method chaining, validation
# Decorator: Test interface compliance, delegation
# Observer: Validate notification mechanism
# Strategy: Check runtime interchangeability
# Repository: Verify abstraction layer

# Pattern-specific test suites
pytest tests/patterns/ || npm run test:patterns || mvn test -Dtest=PatternTests
```

## 🚨 Critical Regression Tests (10-15 minutes)

### 7. **Known Problem Areas**

Based on common refactor issues, always test:

**Parameter Passing & Function Signatures:**

```bash
# Test that refactored functions receive correct parameters
# Verify function call sites weren't broken
# Check keyword arguments vs positional arguments
```

**Configuration & Environment:**

```bash
# Test configuration loading
# Verify environment variables are read
# Check default values work
```

**File I/O & Paths:**

```bash
# Test file reading/writing
# Verify output paths are correct
# Check directory creation
```

**External Dependencies:**

```bash
# Test database connections
# Verify API integrations
# Check third-party service calls
```

### 8. **Cross-Platform/Environment Testing**

```bash
# Test on different environments (dev, staging, prod configs)
# Test with different versions of dependencies
# Test on different operating systems (if applicable)
```

## 📊 Performance & Scale Testing (Optional, 10-20 minutes)

### 9. **Performance Baseline**

```bash
# Measure critical path performance
time your-app performance-critical-operation

# Memory usage check
# CPU profiling (if tools available)
# Database query performance (if applicable)
```

### 10. **Load/Stress Testing** (if applicable)

```bash
# API load testing: Apache Bench, wrk, k6
# Database stress testing
# Concurrent user simulation
```

## 🔧 Refactor-Specific Scenarios

### **API/Interface Changes**

- [ ] All endpoints/methods work
- [ ] Request/response formats unchanged (or properly versioned)
- [ ] Authentication/authorization intact
- [ ] Error responses consistent
- [ ] Documentation updated

### **Database/Storage Changes**

- [ ] Migrations run successfully
- [ ] Data integrity maintained
- [ ] Query performance unchanged
- [ ] Backup/restore procedures work
- [ ] Connection pooling functional

### **Business Logic Changes**

- [ ] Core algorithms produce same results
- [ ] Edge cases handled correctly
- [ ] Validation rules enforced
- [ ] State transitions work
- [ ] Calculations accurate

### **Infrastructure/Deployment Changes**

- [ ] Build process works
- [ ] Deployment scripts functional
- [ ] Environment variables correct
- [ ] Service dependencies available
- [ ] Monitoring/logging intact

### **Security Changes**

- [ ] Authentication mechanisms work
- [ ] Authorization rules enforced
- [ ] Input validation active
- [ ] Encryption/decryption functional
- [ ] Security headers present

## 🎯 Success Criteria

**✅ Ready to Deploy When:**

- [ ] All automated tests pass
- [ ] Code quality checks pass
- [ ] Manual smoke tests work
- [ ] Performance within acceptable range
- [ ] Security measures intact
- [ ] Documentation updated
- [ ] Known critical workflows verified
- [ ] Rollback plan prepared

**❌ Not Ready When:**

- [ ] Any test failures
- [ ] Code quality issues
- [ ] Broken core functionality
- [ ] Performance regressions
- [ ] Security vulnerabilities
- [ ] Missing documentation
- [ ] Incomplete rollback plan

## 🚀 Technology-Specific Quick Commands

### **Python Projects**

```bash
# Full validation
pytest tests/ && mypy src/ && black --check src/ && flake8 src/
python -m your_package --help
```

### **Node.js Projects**

```bash
# Full validation
npm test && npm run lint && npm run type-check
node index.js --help
```

### **Java Projects**

```bash
# Full validation
mvn clean test && mvn checkstyle:check
java -jar target/your-app.jar --help
```

### **Go Projects**

```bash
# Full validation
go test ./... && go vet ./... && gofmt -d .
./your-binary --help
```

### **Rust Projects**

```bash
# Full validation
cargo test && cargo clippy && cargo fmt --check
cargo run -- --help
```

### **Web Applications**

```bash
# Start application
npm start / python manage.py runserver / mvn spring-boot:run

# Test critical endpoints
curl http://localhost:8080/health
curl http://localhost:8080/api/status
```

## 💡 Universal Principles

1. **Test Integration Points**: Most bugs occur where components connect
2. **Verify User Journeys**: Test complete workflows, not just individual functions
3. **Check Parameter Passing**: Function signature changes are common bug sources
4. **Validate External Dependencies**: Database, API, file system interactions
5. **Performance Baseline**: Ensure no significant regressions
6. **Error Handling**: Verify graceful failure modes
7. **Configuration**: Test with different environment settings
8. **Code Style Matters**: Style issues don't break functionality but reduce maintainability and can fail CI/CD

## ⚠️ Common Validation Gaps

### **Why Unit Tests Don't Catch Everything**

- **Code Style Issues**: Tests focus on functionality, not formatting
- **Unused Imports/Variables**: Python allows these at runtime
- **Naming Conventions**: Poor names work but reduce readability
- **Performance Issues**: Tests may pass but run slowly
- **Integration Problems**: Components work alone but not together

### **The Multi-Layer Validation Strategy**

```bash
# Layer 1: Code Style & Formatting
trunk check src/ || ruff check src/ || eslint src/

# Layer 2: Type Safety & Static Analysis
mypy src/ || tsc --noEmit || go vet ./...

# Layer 3: Functional Testing
pytest tests/ || npm test || go test ./...

# Layer 4: Integration & Performance
# Run end-to-end tests and performance benchmarks
```

## 🔗 Adaptation Guidelines

### **For Your Specific Project:**

1. Replace generic commands with your tech stack equivalents
2. Add project-specific critical workflows
3. Include your performance benchmarks
4. Add your deployment/infrastructure checks
5. Include your security requirements
6. Customize the success criteria

### **For Your Team:**

1. Add to CI/CD pipeline where possible
2. Create project-specific test data
3. Document known problem areas
4. Maintain performance baselines
5. Update based on past incidents

---

**Remember**: The goal is to catch integration issues that unit tests miss. Focus on complete user workflows and component interactions. Adapt this checklist to your specific technology stack, project requirements, and team practices.
