# 🤖 Universal Validation Prompts for Any Codebase

Ready-to-use AI prompts based on the Universal Refactor Validation Toolkit. Copy, customize, and use these prompts to validate any codebase after refactoring or code changes.

## 📋 Quick Reference

| **Prompt Type**               | **Use Case**                          | **Time**  | **Complexity** |
| ----------------------------- | ------------------------------------- | --------- | -------------- |
| **🚀 Quick Validation**       | Daily development, small changes      | 5-10 min  | Simple         |
| **🔍 Comprehensive Analysis** | Major refactors, code reviews         | 15-30 min | Medium         |
| **🏗️ Pattern Validation**     | Architecture changes, design patterns | 10-20 min | Medium         |
| **📊 Performance & Scale**    | Optimization, large codebases         | 20-30 min | Complex        |
| **🚨 Critical System Check**  | Production deployments, releases      | 30-45 min | Complex        |
| **🔧 Technology-Specific**    | Language/framework specific issues    | 10-25 min | Variable       |

---

## 🚀 **Quick Validation Prompts**

### **Prompt 1: 5-Minute Sanity Check**

```
I just made changes to my [TECHNOLOGY] codebase. Please help me run a quick 5-minute validation to ensure nothing is broken.

**My Setup:**
- Technology: [Python/JavaScript/Java/Go/Rust/etc.]
- Project Type: [CLI tool/Web API/Library/Desktop app/etc.]
- Changes Made: [Brief description of what you changed]

**Please help me:**
1. Run appropriate linting and code quality checks for my tech stack
2. Test basic functionality (help messages, main entry points)
3. Run unit tests and check for any failures
4. Verify the application still starts/runs correctly

**Focus on catching:**
- Syntax errors or import issues
- Broken basic functionality
- Obvious test failures
- Critical path problems

Provide specific commands I can run for my technology stack.
```

### **Prompt 2: Code Style & Quality Check**

```
I need to validate code style and quality in my [TECHNOLOGY] project after making changes.

**Project Details:**
- Language: [Specific language and version]
- Framework: [If applicable]
- Code Style Tools: [What you have available - eslint, ruff, trunk, etc.]

**Please help me:**
1. Check for code formatting issues
2. Find unused imports and variables
3. Identify naming convention violations
4. Detect potential code smells
5. Validate type annotations (if applicable)

**What I want to avoid:**
- Inconsistent formatting
- Unused code cluttering the codebase
- Poor variable/function names
- Type safety issues
- Style guide violations

Give me the exact commands to run and explain what each one checks.
```

### **Prompt 3: Basic Functionality Validation**

```
I've refactored [SPECIFIC_COMPONENT] in my [TECHNOLOGY] application. Help me verify that basic functionality still works.

**Application Details:**
- Type: [CLI/Web API/Desktop app/Library/etc.]
- Main entry points: [List your main commands/endpoints/functions]
- Critical workflows: [2-3 most important user journeys]

**Please help me test:**
1. Application startup and help systems
2. Main command/API endpoints work
3. Configuration loading
4. Error handling for invalid inputs
5. Basic integration with external services (if any)

**Test scenarios to cover:**
- Happy path: [Describe normal usage]
- Error cases: [Common error scenarios]
- Edge cases: [Boundary conditions]

Provide a systematic testing approach I can follow.
```

---

## 🔍 **Comprehensive Analysis Prompts**

### **Prompt 4: Full Refactor Validation**

```
I've completed a major refactor of [COMPONENT/MODULE] in my [TECHNOLOGY] codebase. Please help me run comprehensive validation to ensure I haven't broken anything.

**Refactor Details:**
- What was changed: [Detailed description]
- Scope: [Files/modules affected]
- Architecture changes: [Any design pattern or structure changes]
- Dependencies modified: [New/removed/updated dependencies]

**My Technology Stack:**
- Language: [Language and version]
- Framework: [Framework and version]
- Database: [If applicable]
- External services: [APIs, cloud services, etc.]

**Please help me systematically validate:**

1. **Code Quality (5-10 min)**
   - Linting and formatting
   - Type checking
   - Import organization
   - Code style compliance

2. **Unit Testing (10-15 min)**
   - All existing tests pass
   - New tests for changed functionality
   - Test coverage maintained
   - Mock/stub validation

3. **Integration Testing (10-15 min)**
   - Component interactions work
   - External service integrations
   - Database operations (if applicable)
   - Configuration loading

4. **End-to-End Validation (10-15 min)**
   - Complete user workflows
   - Error handling paths
   - Performance characteristics
   - Security measures intact

**Critical areas to focus on:**
- Parameter passing between functions
- File I/O and path handling
- External dependency interactions
- Error propagation and handling

Provide a step-by-step checklist with specific commands for my tech stack.
```

### **Prompt 5: Cross-Component Integration Check**

```
I've refactored how [COMPONENT_A] interacts with [COMPONENT_B] in my [TECHNOLOGY] application. Help me validate that all integration points still work correctly.

**Integration Details:**
- Components involved: [List all affected components]
- Interface changes: [API/function signature changes]
- Data flow changes: [How data moves between components]
- Shared dependencies: [Common libraries, databases, services]

**Please help me validate:**

1. **Interface Compatibility**
   - Function signatures match expectations
   - Data types are correctly passed
   - Return values are properly handled
   - Error conditions propagate correctly

2. **Data Flow Integrity**
   - Data transformations work correctly
   - No data loss or corruption
   - Proper validation at boundaries
   - Consistent data formats

3. **Error Handling**
   - Errors are caught and handled appropriately
   - Graceful degradation works
   - Logging and monitoring intact
   - User-friendly error messages

4. **Performance Impact**
   - No significant performance regressions
   - Memory usage remains reasonable
   - Database queries optimized
   - Network calls efficient

**Test scenarios:**
- Normal operation flow
- Error conditions and edge cases
- High load scenarios (if applicable)
- Failure recovery

Give me specific tests to run and metrics to measure.
```

---

## 🏗️ **Design Pattern Validation Prompts**

### **Prompt 6: Pattern Implementation Check**

```
I've implemented/refactored [DESIGN_PATTERN] in my [TECHNOLOGY] codebase. Please help me validate that the pattern is correctly implemented and follows best practices.

**Pattern Details:**
- Pattern Type: [Factory/Builder/Decorator/Observer/Strategy/Repository/etc.]
- Implementation files: [List relevant files]
- Purpose: [Why you're using this pattern]

**Please help me validate:**

**For Factory Pattern:**
- Factory methods return interfaces, not concrete classes
- Client code uses factory instead of direct instantiation
- Factory handles object creation complexity properly
- Factory supports extensibility for new types

**For Builder Pattern:**
- Builder methods return builder instance (method chaining)
- build() method validates required fields
- Builder handles state correctly
- Complex object creation is simplified

**For Decorator Pattern:**
- Decorators implement same interface as component
- Decorators delegate to wrapped component
- Decorators can be stacked/composed
- Original functionality is preserved

**For Observer Pattern:**
- Subject maintains list of observers
- Observers implement notification interface
- Subject notifies all observers on state change
- Observers can register/unregister dynamically

**For Strategy Pattern:**
- Context uses strategy interface, not concrete strategies
- Strategies are interchangeable at runtime
- Context delegates algorithm to strategy
- Strategies encapsulate algorithm variations

**For Repository Pattern:**
- Repository provides collection-like interface
- Repository abstracts data access technology
- Repository methods use domain objects
- Repository supports querying and persistence

**Please check for:**
- Proper interface usage
- Correct delegation patterns
- Appropriate abstraction levels
- Common anti-patterns
- Testing strategies for the pattern

Provide specific validation steps and potential issues to look for.
```

### **Prompt 7: Anti-Pattern Detection**

```
Please help me analyze my [TECHNOLOGY] codebase for common anti-patterns and design issues that might have been introduced during recent changes.

**Codebase Details:**
- Language: [Language and version]
- Size: [Approximate lines of code or file count]
- Architecture: [Monolith/Microservices/Layered/etc.]
- Recent changes: [What was modified]

**Please help me detect:**

1. **God Object Anti-Pattern**
   - Classes with too many responsibilities
   - Files with excessive lines of code
   - Methods doing too many things

2. **Tight Coupling Issues**
   - Excessive dependencies between modules
   - Hard-coded references to concrete classes
   - Difficult to test or mock components

3. **Code Smells**
   - Duplicate code blocks
   - Long parameter lists
   - Deep nesting levels
   - Complex conditional logic

4. **Performance Anti-Patterns**
   - N+1 query problems
   - Inefficient loops or algorithms
   - Memory leaks or excessive allocations
   - Blocking operations in wrong places

5. **Security Anti-Patterns**
   - Hard-coded credentials or secrets
   - Insufficient input validation
   - Improper error message exposure
   - Insecure data handling

**Analysis approach:**
- Static code analysis tools to use
- Metrics to measure (complexity, coupling, etc.)
- Manual review guidelines
- Automated detection strategies

Provide specific tools and commands for my technology stack, plus manual review guidelines.
```

---

## 📊 **Performance & Scale Validation Prompts**

### **Prompt 8: Performance Regression Check**

```
I've made changes to [COMPONENT] in my [TECHNOLOGY] application. Help me validate that there are no performance regressions.

**Application Details:**
- Type: [Web API/CLI tool/Desktop app/etc.]
- Performance-critical operations: [List key operations]
- Current performance baselines: [If you have them]
- Expected load: [Users, requests/sec, data volume, etc.]

**Please help me validate:**

1. **Response Time Analysis**
   - Critical path performance
   - Database query performance
   - API endpoint response times
   - File I/O operations

2. **Resource Usage**
   - Memory consumption patterns
   - CPU utilization
   - Disk I/O efficiency
   - Network bandwidth usage

3. **Scalability Characteristics**
   - Performance under increased load
   - Memory usage growth patterns
   - Connection pooling efficiency
   - Caching effectiveness

4. **Bottleneck Identification**
   - Slow database queries
   - Inefficient algorithms
   - Resource contention points
   - External service dependencies

**Testing approach:**
- Baseline measurements to take
- Load testing scenarios
- Profiling tools to use
- Metrics to monitor

**Performance targets:**
- Acceptable response times
- Memory usage limits
- Throughput requirements
- Scalability goals

Provide specific tools, commands, and testing strategies for my technology stack.
```

### **Prompt 9: Large Codebase Validation**

```
I need to validate changes across a large [TECHNOLOGY] codebase with [APPROXIMATE_SIZE] files. Help me create an efficient validation strategy that scales.

**Codebase Characteristics:**
- Size: [Number of files, lines of code]
- Team size: [Number of developers]
- Architecture: [Monolith/Microservices/Distributed/etc.]
- CI/CD setup: [What you have in place]
- Testing infrastructure: [Current test setup]

**Challenges:**
- Long test execution times
- Complex dependency chains
- Multiple integration points
- Performance considerations
- Resource constraints

**Please help me create:**

1. **Efficient Validation Pipeline**
   - Parallel execution strategies
   - Incremental testing approaches
   - Smart test selection
   - Fast feedback mechanisms

2. **Automated Quality Gates**
   - Code quality thresholds
   - Performance benchmarks
   - Security scan requirements
   - Documentation standards

3. **Risk-Based Testing**
   - Critical path identification
   - High-risk area focus
   - Impact assessment strategies
   - Rollback procedures

4. **Monitoring and Alerting**
   - Key metrics to track
   - Alert thresholds
   - Dashboard setup
   - Incident response

**Optimization strategies:**
- Caching mechanisms
- Parallel processing
- Resource allocation
- Tool selection

Provide a scalable validation framework with specific tools and processes.
```

---

## 🚨 **Critical System Validation Prompts**

### **Prompt 10: Production Readiness Check**

```
I'm preparing to deploy changes to my [TECHNOLOGY] application to production. Help me run a comprehensive production readiness validation.

**Application Details:**
- Type: [Web service/API/Desktop app/etc.]
- Deployment target: [Cloud provider, on-premise, etc.]
- User base: [Size and criticality]
- SLA requirements: [Uptime, performance, etc.]

**Changes being deployed:**
- [Detailed description of changes]
- [Risk assessment of changes]
- [Rollback plan availability]

**Please help me validate:**

1. **Functional Correctness**
   - All features work as expected
   - No regressions in existing functionality
   - New features meet requirements
   - Error handling is robust

2. **Performance & Scalability**
   - Meets performance SLAs
   - Handles expected load
   - Resource usage is acceptable
   - No memory leaks or resource exhaustion

3. **Security & Compliance**
   - Security scans pass
   - Authentication/authorization works
   - Data protection measures intact
   - Compliance requirements met

4. **Operational Readiness**
   - Monitoring and alerting configured
   - Logging is comprehensive
   - Health checks work
   - Deployment process tested

5. **Disaster Recovery**
   - Backup procedures work
   - Rollback plan tested
   - Data recovery procedures
   - Incident response ready

**Critical validations:**
- Database migrations (if any)
- External service integrations
- Configuration management
- Secret management
- Network connectivity

Provide a production deployment checklist with go/no-go criteria.
```

### **Prompt 11: Security & Compliance Validation**

```
Help me validate the security and compliance aspects of my [TECHNOLOGY] application after recent changes.

**Application Context:**
- Industry: [Healthcare/Finance/E-commerce/etc.]
- Compliance requirements: [GDPR/HIPAA/SOX/PCI-DSS/etc.]
- Data sensitivity: [PII/Financial/Health/etc.]
- Threat model: [Key security concerns]

**Recent changes:**
- [What was modified]
- [New features or integrations]
- [Dependencies updated]

**Please help me validate:**

1. **Authentication & Authorization**
   - User authentication mechanisms
   - Role-based access control
   - Session management
   - Multi-factor authentication (if applicable)

2. **Data Protection**
   - Data encryption at rest and in transit
   - PII handling procedures
   - Data retention policies
   - Secure data disposal

3. **Input Validation & Sanitization**
   - SQL injection prevention
   - XSS protection
   - CSRF protection
   - Input validation completeness

4. **Security Configuration**
   - Secure defaults
   - Unnecessary services disabled
   - Security headers configured
   - Error message sanitization

5. **Compliance Requirements**
   - Audit logging
   - Data access controls
   - Privacy controls
   - Regulatory reporting

**Security testing:**
- Vulnerability scanning
- Penetration testing approaches
- Code security analysis
- Dependency vulnerability checks

Provide specific security validation steps and compliance checkpoints.
```

---

## 🔧 **Technology-Specific Prompts**

### **Prompt 12: Python Application Validation**

````
I've made changes to my Python application. Help me run Python-specific validation to ensure code quality and functionality.

**Python Setup:**
- Python version: [3.x]
- Framework: [Django/Flask/FastAPI/CLI/etc.]
- Key dependencies: [List major packages]
- Virtual environment: [Yes/No]

**Please help me run:**

1. **Python-Specific Code Quality**
   ```bash
   # Syntax and compilation
   python -m py_compile [files]

   # Type checking
   mypy src/ --ignore-missing-imports

   # Linting and style
   ruff check src/
   black --check src/
   trunk check src/

   # Security scanning
   bandit -r src/
````

2. **Dependency Management**
   - Requirements file validation
   - Dependency vulnerability scanning
   - Version compatibility checks
   - Virtual environment integrity

3. **Python-Specific Testing**
   - pytest execution with coverage
   - Docstring testing (doctest)
   - Import testing
   - Package structure validation

4. **Performance Analysis**
   - cProfile profiling
   - Memory usage analysis
   - Import time optimization
   - Startup time measurement

**Common Python Issues to Check:**

- Circular imports
- Global variable usage
- Exception handling patterns
- Resource management (context managers)
- Async/await usage (if applicable)

Provide specific commands and Python best practices to validate.

```

### **Prompt 13: JavaScript/Node.js Validation**
```

Help me validate my JavaScript/Node.js application after making changes.

**JavaScript Setup:**

- Runtime: [Node.js version/Browser]
- Framework: [React/Vue/Angular/Express/etc.]
- Package manager: [npm/yarn/pnpm]
- TypeScript: [Yes/No]

**Please help me run:**

1. **JavaScript Code Quality**

   ```bash
   # Linting
   eslint src/ --ext .js,.ts,.jsx,.tsx

   # Type checking (if TypeScript)
   tsc --noEmit

   # Formatting
   prettier --check src/

   # Security scanning
   npm audit
   ```

2. **Dependency Management**
   - Package.json validation
   - Lock file integrity
   - Dependency vulnerability scanning
   - Bundle size analysis

3. **JavaScript Testing**
   - Jest/Mocha test execution
   - Coverage reporting
   - E2E testing (if applicable)
   - Component testing

4. **Performance Analysis**
   - Bundle analysis
   - Runtime performance
   - Memory leak detection
   - Load time optimization

**JavaScript-Specific Issues:**

- Callback hell or Promise chains
- Memory leaks in event listeners
- Prototype pollution
- Async/await error handling
- Module loading patterns

Provide specific validation steps for JavaScript/Node.js applications.

```

### **Prompt 14: Java Application Validation**
```

I need to validate my Java application after recent changes. Help me run Java-specific validation.

**Java Setup:**

- Java version: [8/11/17/21/etc.]
- Framework: [Spring Boot/Quarkus/Plain Java/etc.]
- Build tool: [Maven/Gradle]
- Application server: [If applicable]

**Please help me run:**

1. **Java Code Quality**

   ```bash
   # Compilation
   mvn compile

   # Static analysis
   mvn checkstyle:check
   mvn spotbugs:check
   mvn pmd:check

   # Dependency analysis
   mvn dependency:analyze
   ```

2. **Java Testing**
   - Unit tests (JUnit/TestNG)
   - Integration tests
   - Test coverage (JaCoCo)
   - Performance tests (JMH)

3. **Java-Specific Analysis**
   - Memory usage patterns
   - Garbage collection impact
   - Thread safety analysis
   - Serialization compatibility

4. **Enterprise Concerns**
   - Security manager compliance
   - JMX monitoring setup
   - Logging configuration
   - Connection pooling

**Java-Specific Issues:**

- Memory leaks
- Thread deadlocks
- Serialization problems
- Classpath conflicts
- Resource management

Provide Java-specific validation commands and best practices.

```

---

## 🎯 **Specialized Validation Prompts**

### **Prompt 15: API/Web Service Validation**
```

I've made changes to my [REST API/GraphQL/gRPC] service. Help me validate that the API still works correctly.

**API Details:**

- API type: [REST/GraphQL/gRPC/etc.]
- Technology: [Language and framework]
- Authentication: [JWT/OAuth/API Keys/etc.]
- Documentation: [OpenAPI/Swagger/etc.]

**Please help me validate:**

1. **API Functionality**
   - All endpoints respond correctly
   - Request/response formats unchanged
   - Authentication/authorization works
   - Error responses are appropriate

2. **API Contract Validation**
   - Schema validation
   - Backward compatibility
   - Version compatibility
   - Breaking change detection

3. **Performance Testing**
   - Response time benchmarks
   - Throughput testing
   - Concurrent user handling
   - Rate limiting functionality

4. **Security Testing**
   - Input validation
   - SQL injection protection
   - Authentication bypass attempts
   - Authorization checks

**Testing approach:**

- Automated API testing tools
- Load testing strategies
- Security scanning tools
- Contract testing methods

Provide specific API validation strategies and tools.

```

### **Prompt 16: Database Integration Validation**
```

I've made changes that affect database interactions in my [TECHNOLOGY] application. Help me validate database integration.

**Database Setup:**

- Database type: [PostgreSQL/MySQL/MongoDB/etc.]
- ORM/Query builder: [SQLAlchemy/Hibernate/Mongoose/etc.]
- Migration system: [Alembic/Flyway/etc.]
- Connection pooling: [Yes/No]

**Changes made:**

- [Schema changes]
- [Query modifications]
- [Index changes]
- [Migration scripts]

**Please help me validate:**

1. **Schema Integrity**
   - Migration scripts work correctly
   - Schema changes are backward compatible
   - Constraints are properly enforced
   - Indexes are optimized

2. **Query Performance**
   - Query execution plans
   - Index usage analysis
   - N+1 query detection
   - Slow query identification

3. **Data Integrity**
   - Foreign key constraints
   - Data validation rules
   - Transaction boundaries
   - Rollback procedures

4. **Connection Management**
   - Connection pooling efficiency
   - Connection leak detection
   - Timeout handling
   - Failover mechanisms

**Database-specific testing:**

- Migration testing
- Performance benchmarking
- Data consistency checks
- Backup/restore validation

Provide database-specific validation steps and tools.

```

---

## 📝 **How to Use These Prompts**

### **Step 1: Choose the Right Prompt**
- **Quick changes**: Use Quick Validation prompts (#1-3)
- **Major refactors**: Use Comprehensive Analysis prompts (#4-5)
- **Design changes**: Use Pattern Validation prompts (#6-7)
- **Performance concerns**: Use Performance prompts (#8-9)
- **Production deployment**: Use Critical System prompts (#10-11)
- **Language-specific**: Use Technology-Specific prompts (#12-14)
- **Specialized systems**: Use Specialized prompts (#15-16)

### **Step 2: Customize the Prompt**
1. Replace `[TECHNOLOGY]` with your specific language/framework
2. Fill in `[BRACKETS]` with your project details
3. Add specific context about your changes
4. Include relevant file paths or component names

### **Step 3: Iterate and Refine**
- Start with broader prompts, then get specific
- Use follow-up questions based on initial results
- Combine multiple prompts for comprehensive validation
- Adapt prompts based on what you learn

### **Step 4: Build Your Validation Workflow**
- Create project-specific prompt templates
- Integrate with your CI/CD pipeline
- Document common issues and solutions
- Share successful prompts with your team

---

## 🚀 **Quick Copy-Paste Templates**

### **Daily Development Validation**
```

I made small changes to my [TECHNOLOGY] [PROJECT_TYPE]. Quick validation check:

- Technology: [Fill in]
- Changes: [Brief description]
- Time available: 5 minutes

Help me run: linting, basic tests, functionality check.

```

### **Pre-Commit Validation**
```

About to commit changes to [COMPONENT] in [TECHNOLOGY]. Comprehensive check:

- Files changed: [List files]
- Functionality affected: [Description]
- Risk level: [Low/Medium/High]

Help me validate: code quality, tests, integration, performance.

```

### **Pre-Deployment Validation**
```

Deploying [CHANGES] to production [TECHNOLOGY] app. Critical validation:

- Changes: [Detailed description]
- Risk assessment: [Analysis]
- Rollback plan: [Available/Not available]

Help me validate: functionality, performance, security, operations.

```

---

**Remember**: These prompts are starting points. Customize them for your specific technology stack, project requirements, and team practices. The key is systematic, multi-layer validation that catches issues unit tests miss!
```
