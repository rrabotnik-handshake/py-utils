# 🚨 Anti-Pattern Catalog

**Central index of code anti-patterns and architectural smells to catch during validation**

This catalog helps identify common anti-patterns that traditional testing often misses but can severely impact maintainability, performance, and reliability.

---

## 🎯 **Quick Anti-Pattern Reference**

| **Anti-Pattern**                     | **Severity** | **Detection Time** | **Impact**      | **Auto-Detectable** |
| ------------------------------------ | ------------ | ------------------ | --------------- | ------------------- |
| 🏛️ **God Object**                    | Critical     | 5-10m              | Maintainability | ✅                  |
| 🍝 **Spaghetti Code**                | High         | 10-15m             | Maintainability | ✅                  |
| 💎 **Diamond Problem**               | High         | 5-10m              | Architecture    | ✅                  |
| 🔄 **Circular Dependencies**         | Critical     | 5-10m              | Architecture    | ✅                  |
| 🎭 **Poltergeist**                   | Medium       | 10-15m             | Performance     | ⚠️                  |
| 🌋 **Lava Flow**                     | Medium       | 15-30m             | Maintainability | ⚠️                  |
| 🏭 **Factory of Factories**          | Medium       | 10-15m             | Complexity      | ⚠️                  |
| 🔗 **Chain of Responsibility Abuse** | Medium       | 10-15m             | Performance     | ⚠️                  |
| 🎯 **Golden Hammer**                 | Low          | 15-30m             | Architecture    | ❌                  |
| 📦 **Vendor Lock-in**                | Low          | 15-30m             | Architecture    | ❌                  |

**Legend**: ✅ Fully automated, ⚠️ Partially automated, ❌ Manual detection required

---

## 🏛️ **Architectural Anti-Patterns**

### **🏛️ God Object (God Class)**

**Description**: A class that knows too much or does too much, violating Single Responsibility Principle.

**Detection Criteria**:

- Class has >500 lines of code
- Class has >20 methods
- Class has >15 instance variables
- Class imports >20 different modules

**Automated Detection**:

```bash
# Using validate_patterns.py
python validate_patterns.py ./src --patterns god_object --verbose

# Manual detection
find ./src -name "*.py" -exec wc -l {} + | awk '$1 > 500'
grep -r "class " ./src | grep -E "def.*{20,}" # Approximate
```

**Example**:

```python
# BAD: God Object
class UserManager:
    def __init__(self):
        self.db = Database()
        self.email_service = EmailService()
        self.payment_service = PaymentService()
        self.notification_service = NotificationService()
        # ... 15+ more services

    def create_user(self): pass
    def update_user(self): pass
    def delete_user(self): pass
    def send_email(self): pass
    def process_payment(self): pass
    def send_notification(self): pass
    def generate_report(self): pass
    def backup_data(self): pass
    # ... 20+ more methods
```

**Fix Strategy**:

1. Extract related methods into separate classes
2. Use composition instead of inheritance
3. Apply Single Responsibility Principle
4. Create focused service classes

**🔗 Related Resources**:

- 🤖 **AI Prompt**: [God Object Refactoring](UNIVERSAL_VALIDATION_PROMPTS.md#god-object-refactoring)
- 📊 **Assessment**: Use comprehensive validation to track complexity metrics

---

### **🍝 Spaghetti Code**

**Description**: Code with complex and tangled control flow, making it difficult to follow and maintain.

**Detection Criteria**:

- Cyclomatic complexity >10 per function
- Deeply nested control structures (>4 levels)
- Functions with >50 lines
- Multiple return statements scattered throughout

**Automated Detection**:

```bash
# Using radon for complexity
pip install radon
radon cc ./src -a -nb  # Show functions with complexity > 10

# Using validate_patterns.py
python validate_patterns.py ./src --patterns spaghetti --verbose
```

**Example**:

```python
# BAD: Spaghetti Code
def process_order(order):
    if order.status == "pending":
        if order.payment_method == "credit_card":
            if order.amount > 1000:
                if order.customer.vip_status:
                    if order.items:
                        for item in order.items:
                            if item.stock > 0:
                                if item.category == "electronics":
                                    # ... deeply nested logic continues
                                    return "processed"
                                else:
                                    return "category_error"
                            else:
                                return "out_of_stock"
                    else:
                        return "no_items"
                else:
                    return "not_vip"
            else:
                return "amount_too_low"
        else:
            return "invalid_payment"
    else:
        return "invalid_status"
```

**Fix Strategy**:

1. Extract nested logic into separate functions
2. Use early returns to reduce nesting
3. Apply guard clauses
4. Use strategy pattern for complex conditionals

---

### **🔄 Circular Dependencies**

**Description**: Two or more modules depend on each other directly or indirectly, creating a cycle.

**Detection Criteria**:

- Module A imports Module B, and Module B imports Module A
- Indirect cycles through multiple modules
- Import errors during testing

**Automated Detection**:

```bash
# Using validate_patterns.py
python validate_patterns.py ./src --patterns circular_deps --verbose

# Manual detection with Python
python -c "
import ast
import os
# Custom script to detect circular imports
"
```

**Example**:

```python
# BAD: Circular Dependencies
# user.py
from order import Order

class User:
    def get_orders(self):
        return Order.get_by_user(self.id)

# order.py
from user import User

class Order:
    def get_user(self):
        return User.get_by_id(self.user_id)
```

**Fix Strategy**:

1. Extract common dependencies to a separate module
2. Use dependency injection
3. Apply inversion of control
4. Restructure module hierarchy

---

## 🏗️ **Design Pattern Anti-Patterns**

### **🏭 Factory of Factories**

**Description**: Overuse of factory pattern, creating factories to create other factories.

**Detection Criteria**:

- Factory classes that only create other factory classes
- More than 3 levels of factory nesting
- Factory methods that return factory instances

**Example**:

```python
# BAD: Factory of Factories
class DatabaseFactoryFactory:
    def create_database_factory(self, db_type):
        if db_type == "sql":
            return SQLDatabaseFactory()
        elif db_type == "nosql":
            return NoSQLDatabaseFactory()

class SQLDatabaseFactory:
    def create_connection_factory(self):
        return SQLConnectionFactory()

# This continues for multiple levels...
```

**Fix Strategy**:

1. Simplify factory hierarchy
2. Use configuration-based approach
3. Apply builder pattern for complex construction
4. Consider dependency injection container

---

### **🎭 Poltergeist (Gypsy)**

**Description**: Classes that have very limited responsibilities and are used only to pass information to other classes.

**Detection Criteria**:

- Classes with <3 methods
- Classes that only delegate to other classes
- Classes with no state (only pass-through methods)

**Example**:

```python
# BAD: Poltergeist
class UserValidator:
    def validate(self, user):
        return ValidationService.validate_user(user)

class UserSaver:
    def save(self, user):
        return DatabaseService.save_user(user)
```

**Fix Strategy**:

1. Merge poltergeist functionality into meaningful classes
2. Remove unnecessary abstraction layers
3. Use static methods or functions instead
4. Apply composition over inheritance

---

## 🔄 **Process Anti-Patterns**

### **🌋 Lava Flow**

**Description**: Dead code and forgotten design information that has hardened into an unmovable, useless mass.

**Detection Criteria**:

- Unused functions/classes (detected by static analysis)
- Commented-out code blocks
- TODO comments older than 6 months
- Unreachable code paths

**Automated Detection**:

```bash
# Using vulture to find dead code
pip install vulture
vulture ./src

# Find old TODO comments
grep -r "TODO" ./src --include="*.py" | grep -E "201[0-9]|202[0-2]"

# Using validate_patterns.py
python validate_patterns.py ./src --patterns lava_flow --verbose
```

**Fix Strategy**:

1. Remove dead code systematically
2. Clean up commented-out code
3. Address or remove old TODO items
4. Use feature flags instead of commenting code

---

### **🔗 Chain of Responsibility Abuse**

**Description**: Overuse of Chain of Responsibility pattern, creating unnecessarily long chains.

**Detection Criteria**:

- Chains with >5 handlers
- Handlers that don't follow single responsibility
- Performance issues due to long traversal

**Example**:

```python
# BAD: Chain of Responsibility Abuse
class ValidationChain:
    def __init__(self):
        self.handlers = [
            EmailValidator(),
            PasswordValidator(),
            AgeValidator(),
            AddressValidator(),
            PhoneValidator(),
            CreditCardValidator(),
            TaxIdValidator(),
            # ... 10+ more validators
        ]
```

**Fix Strategy**:

1. Group related validations
2. Use composite pattern
3. Implement parallel validation
4. Consider strategy pattern instead

---

## 🎯 **Behavioral Anti-Patterns**

### **🎯 Golden Hammer**

**Description**: Over-reliance on a familiar technology or pattern for all problems.

**Detection Criteria**:

- Same pattern used in >80% of classes
- Technology choice doesn't fit problem domain
- Ignoring better alternatives

**Manual Detection Questions**:

- Are we using the same design pattern everywhere?
- Is our technology choice appropriate for each use case?
- Are we avoiding learning new approaches?

**Fix Strategy**:

1. Evaluate each problem independently
2. Research alternative solutions
3. Prototype different approaches
4. Apply appropriate patterns per context

---

### **📦 Vendor Lock-in**

**Description**: Excessive dependence on a particular vendor's products or services.

**Detection Criteria**:

- Direct use of vendor-specific APIs throughout codebase
- No abstraction layer for external services
- Vendor-specific data formats

**Example**:

```python
# BAD: Vendor Lock-in
import boto3  # AWS specific

class UserService:
    def __init__(self):
        self.s3 = boto3.client('s3')  # Direct AWS dependency
        self.dynamodb = boto3.resource('dynamodb')

    def save_user_photo(self, photo):
        self.s3.put_object(Bucket='users', Key=photo.name, Body=photo.data)
```

**Fix Strategy**:

1. Create abstraction layers
2. Use adapter pattern
3. Implement interface-based design
4. Consider multi-cloud strategies

---

## 🔍 **Detection Automation**

### **Integration with validate_patterns.py**

```python
# Enhanced pattern detection
python validate_patterns.py ./src \
    --patterns god_object,spaghetti,circular_deps,lava_flow \
    --severity critical,high \
    --output-format json \
    --verbose
```

### **CI/CD Integration**

```yaml
# GitHub Actions example
- name: Anti-Pattern Detection
  run: |
    python validate_patterns.py ./src --patterns all --fail-on critical
    radon cc ./src -a -nb --min B  # Fail on complexity > B grade
    vulture ./src --min-confidence 80  # Find dead code
```

### **Pre-commit Hook**

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: anti-pattern-check
      name: Anti-Pattern Detection
      entry: python validate_patterns.py
      language: system
      args: ["./src", "--patterns", "god_object,spaghetti,circular_deps"]
      pass_filenames: false
```

---

## 📊 **Severity Classification**

### **Critical (Block Deployment)**

- God Object
- Circular Dependencies
- Major Security Anti-patterns

### **High (Requires Fix)**

- Spaghetti Code
- Diamond Problem
- Performance Anti-patterns

### **Medium (Technical Debt)**

- Poltergeist
- Lava Flow
- Factory of Factories

### **Low (Improvement Opportunity)**

- Golden Hammer
- Vendor Lock-in
- Minor Design Issues

---

## 🎯 **Validation Integration**

### **Quick Validation (5-10 minutes)**

- Focus on Critical and High severity anti-patterns
- Automated detection only
- Fast feedback for daily development

### **Comprehensive Validation (30-45 minutes)**

- All severity levels
- Manual review of Medium/Low patterns
- Detailed analysis and recommendations

### **Architecture Review (1-2 hours)**

- Deep dive into architectural anti-patterns
- Cross-cutting concern analysis
- Long-term technical debt assessment

---

## 🔗 **Cross-References**

### **Related Toolkit Resources**

- 🏗️ **Pattern Validation**: [DESIGN_PATTERN_VALIDATION.md](DESIGN_PATTERN_VALIDATION.md)
- 🤖 **AI Prompts**: [UNIVERSAL_VALIDATION_PROMPTS.md](UNIVERSAL_VALIDATION_PROMPTS.md)
- 📊 **Assessment Examples**: [ASSESSMENT_EXAMPLES.md](ASSESSMENT_EXAMPLES.md)
- 🐍 **Automated Tool**: [validate_patterns.py](validate_patterns.py)

### **External Resources**

- [Refactoring Guru - Code Smells](https://refactoring.guru/refactoring/smells)
- [Martin Fowler - Refactoring](https://martinfowler.com/books/refactoring.html)
- [Clean Code Principles](https://clean-code-developer.com/)

---

**This catalog serves as a comprehensive reference for identifying and addressing anti-patterns that can severely impact code quality, maintainability, and system performance. Use it in conjunction with the validation tools and assessment templates for maximum effectiveness.**
