# 🤖 Universal AI Assistant Prompt: Post-Refactor Validation

Use this prompt template when asking an AI assistant to help validate any codebase after refactoring. Customize the bracketed sections for your specific project.

---

## 📋 Prompt Template

```
I've just completed a [REFACTOR_TYPE] in my [TECHNOLOGY_STACK] [PROJECT_TYPE] project. Please help me run comprehensive sanity checks to ensure nothing is broken.

**Project Context:**
- Technology Stack: [Python/JavaScript/Java/Go/Rust/etc.]
- Project Type: [CLI tool/Web API/Library/Desktop app/Mobile app/etc.]
- Main functionality: [Brief description of what your project does]
- Architecture: [Monolith/Microservices/Serverless/etc.]
- Recent changes: [Specific description of what was refactored]
- Critical workflows: [List 2-3 most important user journeys]

**Please help me systematically validate:**

1. **Quick Validation (5-10 min)**
   - Run appropriate linting and type checking for my tech stack
   - Test basic functionality and entry points
   - Verify help messages, API endpoints, or core interfaces

2. **Comprehensive Testing (15-30 min)**
   - Run existing test suite (unit + integration)
   - Identify any test failures or coverage gaps
   - Test end-to-end user workflows
   - Validate critical business logic

3. **Integration Points (10-15 min)**
   - Verify component connections work correctly
   - Test parameter passing between functions/modules
   - Validate external dependencies (database, APIs, file system)
   - Check configuration loading and environment variables
   - Test error handling and edge cases

4. **Design Pattern Validation (5-10 min, if applicable)**
   - Validate proper implementation of design patterns in use
   - Check Factory patterns return interfaces, not concrete classes
   - Verify Builder patterns support method chaining and validation
   - Test Decorator patterns maintain interface compliance and delegation
   - Validate Observer patterns properly notify all observers
   - Check Strategy patterns are truly interchangeable at runtime
   - Ensure Repository patterns abstract data access properly

5. **Performance & Regression Prevention**
   - Check for performance regressions in critical paths
   - Test known problem areas from past issues
   - Verify backward compatibility (if applicable)
   - Validate security measures are intact

5. **Final Assessment Generation (REQUIRED)**
   - Generate structured validation report
   - Provide clear go/no-go recommendation
   - Include risk assessment and next steps
   - Document validation results for future reference

**Focus Areas Based on Refactor Type:**

[Choose relevant sections:]

**For API/Interface Changes:**
- Endpoint functionality and response formats
- Authentication/authorization
- Request validation and error handling
- API documentation accuracy

**For Database/Storage Changes:**
- Data integrity and migration success
- Query performance
- Connection handling
- Backup/restore procedures

**For Business Logic Changes:**
- Algorithm correctness and edge cases
- State management and transitions
- Validation rules and calculations
- Workflow completeness

**For Infrastructure/Deployment Changes:**
- Build and deployment processes
- Environment configuration
- Service dependencies
- Monitoring and logging

**Success Criteria:**
- All tests pass (unit + integration + functional)
- No linting/type errors
- Critical workflows work end-to-end
- No performance regressions
- External integrations functional
- Security measures intact
- Documentation accurate

**Technology-Specific Needs:**
[Add your specific requirements, e.g.:]
- Database migrations successful
- Docker containers build and run
- Kubernetes deployments work
- CI/CD pipeline passes
- Security scans clean

Please provide specific commands to run for my tech stack and help me identify any issues that need fixing. Focus especially on integration points where components connect, as these are where refactor bugs commonly hide.

**IMPORTANT: Always conclude with a structured final assessment report including:**
- Validation results summary
- Production readiness evaluation
- Clear go/no-go recommendation
- Risk assessment and next steps
```

---

## 🎯 Example Usage Scenarios

### **Python Web API Refactor**

```
I've just completed a database layer refactor in my Python FastAPI microservice project. Please help me run comprehensive sanity checks to ensure nothing is broken.

**Project Context:**
- Technology Stack: Python 3.11, FastAPI, SQLAlchemy, PostgreSQL, Redis
- Project Type: REST API microservice
- Main functionality: User management and authentication service
- Architecture: Microservices with Docker containers
- Recent changes: Refactored database models and repository pattern
- Critical workflows: User registration, login, profile updates

[... rest of template ...]
```

### **React Frontend Refactor**

```
I've just completed a component architecture refactor in my React TypeScript web application. Please help me run comprehensive sanity checks to ensure nothing is broken.

**Project Context:**
- Technology Stack: React 18, TypeScript, Next.js, Tailwind CSS
- Project Type: Single-page web application
- Main functionality: E-commerce shopping platform
- Architecture: JAMstack with serverless backend
- Recent changes: Converted class components to hooks, restructured state management
- Critical workflows: Product browsing, cart management, checkout process

[... rest of template ...]
```

### **Java Microservice Refactor**

```
I've just completed a service layer refactor in my Java Spring Boot microservice. Please help me run comprehensive sanity checks to ensure nothing is broken.

**Project Context:**
- Technology Stack: Java 17, Spring Boot, Spring Data JPA, MySQL, RabbitMQ
- Project Type: Microservice in larger distributed system
- Main functionality: Order processing and inventory management
- Architecture: Event-driven microservices
- Recent changes: Refactored business logic and event handling
- Critical workflows: Order creation, inventory updates, payment processing

[... rest of template ...]
```

### **Go CLI Tool Refactor**

```
I've just completed a command structure refactor in my Go CLI application. Please help me run comprehensive sanity checks to ensure nothing is broken.

**Project Context:**
- Technology Stack: Go 1.21, Cobra CLI, GORM, SQLite
- Project Type: Command-line tool
- Main functionality: Database migration and backup utility
- Architecture: Single binary with plugin system
- Recent changes: Restructured command hierarchy and flag handling
- Critical workflows: Database backup, schema migration, data export

[... rest of template ...]
```

---

## 🔧 Customization Guide

### **Technology Stack Adaptations**

**For Python Projects:**

- Add: pytest, mypy, black, flake8/ruff
- Include: virtual environment activation
- Consider: Django/Flask specific checks

**For JavaScript/Node.js:**

- Add: npm test, eslint, prettier, tsc
- Include: package.json scripts
- Consider: React/Vue/Angular specific checks

**For Java Projects:**

- Add: mvn test, checkstyle, spotbugs
- Include: Maven/Gradle commands
- Consider: Spring Boot specific checks

**For Go Projects:**

- Add: go test, go vet, gofmt
- Include: module and dependency checks
- Consider: goroutine and race condition tests

**For Rust Projects:**

- Add: cargo test, clippy, rustfmt
- Include: dependency and feature checks
- Consider: unsafe code validation

### **Project Type Adaptations**

**For Web APIs:**

- Add endpoint testing with curl/Postman
- Include load testing considerations
- Add database connection validation

**For CLI Tools:**

- Add command-line interface testing
- Include help message validation
- Add file I/O and permission checks

**For Libraries:**

- Add API compatibility testing
- Include documentation generation
- Add example usage validation

**For Web Applications:**

- Add browser testing considerations
- Include accessibility checks
- Add performance metrics

### **Architecture Adaptations**

**For Microservices:**

- Add service-to-service communication tests
- Include container and orchestration checks
- Add distributed system health validation

**For Monoliths:**

- Add comprehensive integration testing
- Include database migration validation
- Add deployment pipeline checks

**For Serverless:**

- Add function deployment testing
- Include cold start performance checks
- Add event trigger validation

---

## 💡 Key Principles for Any Technology

1. **Integration Over Isolation**: Focus on how components work together
2. **User Journey Validation**: Test complete workflows, not just individual functions
3. **External Dependency Verification**: Database, APIs, file systems, etc.
4. **Performance Baseline Maintenance**: Ensure no significant regressions
5. **Error Handling Validation**: Test failure modes and edge cases
6. **Configuration Flexibility**: Test with different environment settings
7. **Security Posture Maintenance**: Ensure security measures remain intact

---

## 🚀 Quick Reference Commands by Technology

### **Python**

```bash
pytest tests/ && mypy src/ && black --check src/ && flake8 src/
python -m your_package --help
```

### **JavaScript/Node.js**

```bash
npm test && npm run lint && npm run type-check
node index.js --help
```

### **Java**

```bash
mvn clean test && mvn checkstyle:check
java -jar target/your-app.jar --help
```

### **Go**

```bash
go test ./... && go vet ./... && gofmt -d .
./your-binary --help
```

### **Rust**

```bash
cargo test && cargo clippy && cargo fmt --check
cargo run -- --help
```

---

**Remember**: Adapt this template to your specific needs. The key is systematic validation of integration points where refactor bugs commonly hide. Focus on complete user workflows rather than isolated component testing.
