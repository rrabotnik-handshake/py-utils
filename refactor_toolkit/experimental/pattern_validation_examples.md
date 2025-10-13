# 🏗️ Design Pattern Validation Examples

Practical examples of how to integrate design pattern validation into your refactor workflow.

## 🚀 Quick Usage Examples

### **Basic Pattern Validation**

```bash
# Auto-detect language and validate all patterns
python validate_patterns.py ./src --auto-detect

# Validate specific language
python validate_patterns.py ./src --language python

# Validate only specific patterns
python validate_patterns.py ./src --language java --patterns factory,builder

# Verbose output with exit codes for CI/CD
python validate_patterns.py ./src --auto-detect --verbose --exit-code
```

### **Integration with Existing Workflows**

**Add to your refactor checklist:**

```bash
# After unit tests, before integration tests
echo "🏗️ Validating Design Patterns..."
python validate_patterns.py ./src --auto-detect --exit-code

# If patterns fail, stop the workflow
if [ $? -ne 0 ]; then
    echo "❌ Pattern validation failed. Please fix issues before proceeding."
    exit 1
fi
```

**Add to pre-commit hooks:**

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: validate-patterns
        name: Design Pattern Validation
        entry: python validate_patterns.py
        args: [./src, --auto-detect, --exit-code]
        language: system
        pass_filenames: false
```

**Add to CI/CD pipeline:**

```yaml
# GitHub Actions example
- name: Validate Design Patterns
  run: |
    python validate_patterns.py ./src --auto-detect --verbose --exit-code
```

## 📋 Pattern-Specific Examples

### **Factory Pattern Issues**

**❌ Bad Implementation:**

```python
# factory.py
class UserFactory:
    def create_user(self, user_type):
        if user_type == "admin":
            return AdminUser()  # ❌ Returns concrete class
        elif user_type == "regular":
            return RegularUser()  # ❌ Returns concrete class

# client.py
user = AdminUser()  # ❌ Bypassing factory
```

**✅ Good Implementation:**

```python
# factory.py
class UserFactory:
    def create_user(self, user_type) -> User:  # ✅ Returns interface
        if user_type == "admin":
            return AdminUser()
        elif user_type == "regular":
            return RegularUser()

# client.py
factory = UserFactory()
user = factory.create_user("admin")  # ✅ Using factory
```

### **Builder Pattern Issues**

**❌ Bad Implementation:**

```python
class QueryBuilder:
    def __init__(self):
        self.query = ""

    def select(self, fields):
        self.query += f"SELECT {fields} "
        # ❌ No return self

    def from_table(self, table):
        self.query += f"FROM {table} "
        # ❌ No return self

    def build(self):
        # ❌ No validation
        return self.query
```

**✅ Good Implementation:**

```python
class QueryBuilder:
    def __init__(self):
        self.query = ""
        self._has_select = False
        self._has_from = False

    def select(self, fields):
        self.query += f"SELECT {fields} "
        self._has_select = True
        return self  # ✅ Method chaining

    def from_table(self, table):
        self.query += f"FROM {table} "
        self._has_from = True
        return self  # ✅ Method chaining

    def build(self):
        # ✅ Validation
        if not self._has_select or not self._has_from:
            raise ValueError("Query must have SELECT and FROM")
        return self.query
```

### **Decorator Pattern Issues**

**❌ Bad Implementation:**

```python
class LoggingDecorator:
    def __init__(self, component):
        self.component = component

    def operation(self):
        print("Logging...")
        # ❌ No delegation to wrapped component
        return "logged result"
```

**✅ Good Implementation:**

```python
class LoggingDecorator(ComponentInterface):  # ✅ Implements interface
    def __init__(self, component: ComponentInterface):
        self.component = component

    def operation(self):
        print("Logging...")
        result = self.component.operation()  # ✅ Delegates to wrapped
        print(f"Result: {result}")
        return result
```

## 🔧 Custom Pattern Rules

### **Create Project-Specific Rules**

```python
# custom_pattern_rules.py
def validate_custom_patterns(file_path: str, content: str) -> List[PatternIssue]:
    """Add your project-specific pattern validations"""
    issues = []

    # Example: Enforce repository naming convention
    if "Repository" in content:
        if not re.search(r'class\s+\w+Repository\s*:', content):
            issues.append(PatternIssue(
                pattern=PatternType.REPOSITORY,
                file_path=file_path,
                line_number=1,
                issue_type="naming_convention",
                description="Repository classes must end with 'Repository'",
                severity="warning"
            ))

    # Example: Enforce factory method naming
    if "Factory" in content:
        if not re.search(r'def\s+(create|make|build)_\w+', content):
            issues.append(PatternIssue(
                pattern=PatternType.FACTORY,
                file_path=file_path,
                line_number=1,
                issue_type="method_naming",
                description="Factory methods should start with 'create_', 'make_', or 'build_'",
                severity="info"
            ))

    return issues
```

### **Technology-Specific Adaptations**

**Java Spring Projects:**

```bash
# Check for Spring-specific patterns
grep -r "@Component\|@Service\|@Repository" src/ | wc -l
grep -r "@Autowired" src/ | grep "new " && echo "❌ Manual instantiation with @Autowired"
```

**React Projects:**

```bash
# Check for React patterns
grep -r "Higher.*Order.*Component\|HOC" src/
grep -r "render.*props\|children.*function" src/
```

**Express.js Projects:**

```bash
# Check for middleware patterns
grep -r "app\.use\|router\.use" src/
grep -r "next()" src/ | grep -v "next(error)" && echo "⚠️ Check error handling"
```

## 📊 Pattern Health Metrics

### **Measuring Pattern Usage**

```bash
# Count pattern implementations
echo "Factory patterns: $(grep -r "Factory" src/ | wc -l)"
echo "Builder patterns: $(grep -r "Builder" src/ | wc -l)"
echo "Decorator patterns: $(grep -r "Decorator" src/ | wc -l)"

# Measure coupling
echo "Direct instantiations: $(grep -r "new [A-Z]" src/ | wc -l)"
echo "Interface usage: $(grep -r "implements\|extends" src/ | wc -l)"
```

### **Pattern Complexity Analysis**

```bash
# Find complex classes (potential pattern candidates)
find src/ -name "*.py" -exec wc -l {} + | sort -n | tail -10

# Find classes with many dependencies
grep -r "import\|from" src/ | cut -d: -f1 | sort | uniq -c | sort -n | tail -10
```

## 🎯 Integration with Testing

### **Pattern-Specific Tests**

```python
# test_patterns.py
def test_factory_returns_interface():
    """Test that factories return interface types"""
    factory = UserFactory()
    user = factory.create_user("admin")
    assert isinstance(user, UserInterface)

def test_builder_method_chaining():
    """Test that builder supports method chaining"""
    builder = QueryBuilder()
    result = builder.select("*").from_table("users").where("active=1")
    assert isinstance(result, QueryBuilder)

def test_decorator_delegation():
    """Test that decorators delegate to wrapped component"""
    component = Mock()
    decorator = LoggingDecorator(component)
    decorator.operation()
    component.operation.assert_called_once()

def test_observer_notification():
    """Test that observers are notified of changes"""
    subject = Subject()
    observer = Mock()
    subject.attach(observer)
    subject.notify()
    observer.update.assert_called_once()
```

### **Automated Pattern Testing**

```bash
# Run pattern-specific test suites
pytest tests/patterns/ -v
npm run test:patterns
mvn test -Dtest=PatternTests
```

## 🚨 Common Anti-Patterns to Avoid

### **God Object**

```bash
# Find potentially oversized classes
find src/ -name "*.py" -exec wc -l {} + | awk '$1 > 500 {print $2 " has " $1 " lines"}'
```

### **Tight Coupling**

```bash
# Find excessive concrete dependencies
grep -r "from.*\.concrete\|import.*concrete" src/
```

### **Feature Envy**

```bash
# Manual review needed - look for methods that use other classes more than their own
# Example: methods with many external method calls
```

---

## 💡 Best Practices Summary

1. **Start Simple**: Don't over-engineer with patterns unless they solve real problems
2. **Test Pattern Behavior**: Write tests that verify pattern contracts
3. **Measure Pattern Health**: Track coupling, cohesion, and complexity metrics
4. **Automate Validation**: Integrate pattern checks into CI/CD pipelines
5. **Document Patterns**: Make pattern usage clear for team members
6. **Refactor Gradually**: Introduce patterns incrementally during refactoring
7. **Review Pattern Fit**: Regularly assess if patterns still serve their purpose

**Remember**: Patterns are tools to solve problems, not goals in themselves. Use them when they genuinely improve code quality, maintainability, and team productivity.
