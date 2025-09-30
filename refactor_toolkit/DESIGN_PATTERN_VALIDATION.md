# 🏗️ Design Pattern Validation Checklist

Add this to your post-refactor validation when working with common design patterns. Helps ensure proper implementation and catch architectural issues.

## 🎯 Pattern-Specific Validation

### **🏭 Factory Pattern**

**🔗 Related Resources:**

- 🤖 **AI Prompt**: [Factory Pattern Validation](UNIVERSAL_VALIDATION_PROMPTS.md#factory-pattern-validation)
- 📊 **Assessment Example**: [Pattern-Focused Assessment](ASSESSMENT_EXAMPLES.md#pattern-focused-assessment-examples)
- 🐍 **Automated Check**: `python validate_patterns.py ./src --patterns factory`

**What to Check:**

- [ ] Factory methods return interface/abstract types, not concrete classes
- [ ] Client code depends on factory interface, not concrete factory
- [ ] Factory handles object creation complexity (parameters, initialization)
- [ ] Factory supports extensibility (new product types)

**Validation Commands:**

```bash
# Check for proper abstraction
grep -r "new ConcreteClass" src/  # Should be minimal
grep -r "Factory.*interface\|abstract" src/  # Should exist

# Test factory extensibility
[run-tests] factory_tests/
```

**Common Issues After Refactor:**

- Factory returns concrete types instead of interfaces
- Client code bypasses factory and creates objects directly
- Factory becomes too complex (violates Single Responsibility)
- Missing factory interface (hard to mock/test)

### **🏗️ Builder Pattern**

**🔗 Related Resources:**

- 🤖 **AI Prompt**: [Builder Pattern Validation](UNIVERSAL_VALIDATION_PROMPTS.md#builder-pattern-validation)
- 📊 **Assessment Example**: [Poorly Implemented Builder](ASSESSMENT_EXAMPLES.md#example-2-poorly-implemented-builder-pattern)
- 🐍 **Automated Check**: `python validate_patterns.py ./src --patterns builder`

**What to Check:**

- [ ] Builder methods return builder instance (method chaining)
- [ ] `build()` method validates required fields
- [ ] Builder is immutable or properly handles state
- [ ] Complex object creation is simplified for clients

**Validation Commands:**

```bash
# Check method chaining
grep -A5 "class.*Builder" src/ | grep "return this\|return self"

# Check validation in build method
grep -A10 "def build\|build()" src/ | grep -E "validate|required|check"
```

**Test Scenarios:**

```python
# Test method chaining
builder = MyBuilder().setA(1).setB(2).setC(3)

# Test validation
try:
    incomplete_builder.build()  # Should fail
except ValidationError:
    pass  # Expected

# Test immutability (if applicable)
builder1 = MyBuilder().setA(1)
builder2 = builder1.setB(2)  # Should not affect builder1
```

### **🎨 Decorator Pattern**

**🔗 Related Resources:**

- 🤖 **AI Prompt**: [Decorator Pattern Validation](UNIVERSAL_VALIDATION_PROMPTS.md#decorator-pattern-validation)
- 📊 **Assessment Example**: [Pattern-Focused Assessment](ASSESSMENT_EXAMPLES.md#pattern-focused-assessment-examples)
- 🐍 **Automated Check**: `python validate_patterns.py ./src --patterns decorator`

**What to Check:**

- [ ] Decorators implement same interface as component
- [ ] Decorators delegate to wrapped component
- [ ] Decorators can be stacked/composed
- [ ] Original functionality preserved

**Validation Commands:**

```bash
# Check interface compliance
grep -r "implements\|extends\|class.*Decorator" src/

# Check delegation patterns
grep -A5 "class.*Decorator" src/ | grep -E "\.component\.|\.wrapped\."
```

**Test Scenarios:**

```python
# Test interface compliance
original = ConcreteComponent()
decorated = DecoratorA(DecoratorB(original))
assert isinstance(decorated, ComponentInterface)

# Test stacking
result1 = original.operation()
result2 = decorated.operation()
# Verify enhanced behavior while preserving core functionality

# Test delegation
mock_component = Mock()
decorator = ConcreteDecorator(mock_component)
decorator.operation()
mock_component.operation.assert_called_once()
```

### **👁️ Observer Pattern**

**What to Check:**

- [ ] Subject maintains list of observers
- [ ] Observers implement notification interface
- [ ] Subject notifies all observers on state change
- [ ] Observers can register/unregister dynamically

**Validation Commands:**

```bash
# Check observer interface
grep -r "Observer\|Listener" src/ | grep "interface\|abstract"

# Check notification mechanism
grep -r "notify\|update\|on.*Changed" src/
```

**Test Scenarios:**

```python
# Test registration/notification
subject = ConcreteSubject()
observer1 = Mock()
observer2 = Mock()

subject.attach(observer1)
subject.attach(observer2)
subject.notify()

observer1.update.assert_called_once()
observer2.update.assert_called_once()

# Test unregistration
subject.detach(observer1)
subject.notify()
observer1.update.assert_called_once()  # Still once
observer2.update.assert_called()  # Called again
```

### **🔄 Strategy Pattern**

**What to Check:**

- [ ] Context uses strategy interface, not concrete strategies
- [ ] Strategies are interchangeable at runtime
- [ ] Context delegates algorithm to strategy
- [ ] Strategies encapsulate algorithm variations

**Validation Commands:**

```bash
# Check strategy interface usage
grep -r "Strategy.*interface\|abstract.*Strategy" src/

# Check runtime strategy switching
grep -r "setStrategy\|changeStrategy" src/
```

**Test Scenarios:**

```python
# Test strategy switching
context = Context(StrategyA())
result1 = context.execute()

context.set_strategy(StrategyB())
result2 = context.execute()

assert result1 != result2  # Different strategies, different results

# Test strategy interface compliance
strategies = [StrategyA(), StrategyB(), StrategyC()]
for strategy in strategies:
    assert isinstance(strategy, StrategyInterface)
    context.set_strategy(strategy)
    result = context.execute()  # Should work with any strategy
```

### **🔗 Command Pattern**

**What to Check:**

- [ ] Commands encapsulate request as object
- [ ] Commands implement execute() method
- [ ] Commands support undo (if applicable)
- [ ] Invoker doesn't know about concrete commands

**Validation Commands:**

```bash
# Check command interface
grep -r "Command.*interface\|abstract.*Command" src/

# Check execute method
grep -r "def execute\|execute()" src/

# Check undo support (if applicable)
grep -r "def undo\|undo()" src/
```

### **🏪 Repository Pattern**

**What to Check:**

- [ ] Repository provides collection-like interface
- [ ] Repository abstracts data access technology
- [ ] Repository methods use domain objects
- [ ] Repository supports querying and persistence

**Validation Commands:**

```bash
# Check repository interface
grep -r "Repository.*interface\|abstract.*Repository" src/

# Check domain object usage (not DTOs/database entities)
grep -A5 "class.*Repository" src/ | grep -v "DTO\|Entity\|Model"
```

## 🧪 Pattern Integration Testing

### **Cross-Pattern Validation**

```bash
# Test pattern combinations
# Factory + Strategy: Factory creates different strategies
# Decorator + Observer: Decorated objects notify observers
# Command + Factory: Factory creates different commands
```

### **Anti-Pattern Detection**

```bash
# God Object (violates Single Responsibility)
wc -l src/**/*.py | sort -n | tail -5  # Find largest files
grep -c "def \|function " src/**/*.py | sort -t: -k2 -n | tail -5  # Most methods

# Tight Coupling
grep -r "import.*\.concrete\|from.*concrete" src/  # Importing concrete classes
grep -r "new \|instanceof " src/ | wc -l  # High concrete instantiation

# Feature Envy (method uses another class more than its own)
# Manual review needed - look for methods with many external calls
```

## 🔍 Automated Pattern Analysis

### **Static Analysis Tools**

**Python:**

```bash
# Check for pattern violations
pylint --load-plugins=pylint_patterns src/
bandit -r src/  # Security patterns
vulture src/  # Dead code (unused pattern implementations)
```

**Java:**

```bash
# Pattern analysis
checkstyle -c pattern_rules.xml src/
spotbugs -effort:max src/
pmd -d src/ -R rulesets/design.xml
```

**JavaScript/TypeScript:**

```bash
# Pattern linting
eslint --ext .js,.ts src/ --config .eslintrc-patterns.js
tslint -c tslint-patterns.json src/**/*.ts
```

### **Custom Pattern Checks**

**Create pattern-specific linters:**

```python
# Example: Factory Pattern Checker
def check_factory_pattern(file_path):
    with open(file_path) as f:
        content = f.read()

    issues = []

    # Check if factory returns interfaces
    if 'class.*Factory' in content and 'return new' in content:
        if not re.search(r'return.*Interface|return.*Abstract', content):
            issues.append("Factory should return interface/abstract types")

    # Check if clients use factory
    if 'new ConcreteClass' in content and 'Factory' not in content:
        issues.append("Consider using factory instead of direct instantiation")

    return issues
```

## 📊 Pattern Health Metrics

### **Measurable Indicators**

**Coupling Metrics:**

```bash
# Afferent/Efferent coupling
# Count incoming/outgoing dependencies per class
grep -r "import\|from" src/ | wc -l
```

**Cohesion Metrics:**

```bash
# LCOM (Lack of Cohesion of Methods)
# Methods should use instance variables
# High LCOM indicates potential pattern violations
```

**Pattern Compliance Score:**

- Factory: % of object creation through factories
- Strategy: % of algorithms encapsulated in strategies
- Observer: % of state changes that notify observers
- Decorator: % of enhancements through decoration vs inheritance

## 🚨 Pattern-Specific Red Flags

### **Factory Pattern Issues**

- [ ] Clients creating objects with `new` instead of factory
- [ ] Factory returning concrete types instead of interfaces
- [ ] Factory becoming too complex (doing more than creation)
- [ ] Missing factory abstraction (can't swap factories)

### **Builder Pattern Issues**

- [ ] Builder methods not returning `this`/`self`
- [ ] Missing validation in `build()` method
- [ ] Builder state mutation after `build()` called
- [ ] Complex objects still constructed directly

### **Decorator Pattern Issues**

- [ ] Decorators not implementing component interface
- [ ] Decorators not delegating to wrapped component
- [ ] Decorators can't be stacked/composed
- [ ] Core functionality broken by decoration

### **Observer Pattern Issues**

- [ ] Observers not properly decoupled from subject
- [ ] Memory leaks from unregistered observers
- [ ] Notification order dependencies
- [ ] Observer exceptions breaking notification chain

### **Strategy Pattern Issues**

- [ ] Context tightly coupled to concrete strategies
- [ ] Strategies sharing state (should be stateless)
- [ ] Strategy selection logic in wrong place
- [ ] Strategies not truly interchangeable

## 🎯 Pattern Refactor Validation Workflow

### **Pre-Refactor Pattern Assessment**

1. Identify current patterns in use
2. Document pattern responsibilities
3. Create pattern-specific tests
4. Establish pattern health metrics

### **Post-Refactor Pattern Validation**

1. Run pattern-specific test suites
2. Check pattern compliance with static analysis
3. Validate pattern interactions
4. Measure pattern health metrics
5. Review for anti-patterns

### **Pattern Evolution Checks**

- [ ] New patterns properly implemented
- [ ] Existing patterns not broken
- [ ] Pattern combinations work correctly
- [ ] No accidental anti-pattern introduction

---

## 🚀 Integration with Main Checklist

Add these pattern checks to your main refactor validation:

```bash
# After unit tests, before integration tests
echo "🏗️ Validating Design Patterns..."

# Run pattern-specific checks
./scripts/check_factory_pattern.sh
./scripts/check_builder_pattern.sh
./scripts/validate_decorators.sh

# Pattern integration tests
pytest tests/patterns/
```

**Remember**: Patterns should solve problems, not create complexity. If a pattern makes code harder to understand or maintain, consider simpler alternatives.
