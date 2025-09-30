# ⚡ Refactor Validation Cheat Sheet

Quick reference for post-refactor validation. Adapt commands to your tech stack.

## 🚀 5-Minute Quick Check

```bash
# 1. Code Quality (MULTI-LAYER!)
[linter] && [type-checker] && [formatter --check] && [style-checker]

# 2. Basic Functionality
[your-app] --help && [your-app] [basic-command]

# 3. Unit Tests
[test-runner] [test-directory]
```

## 🎯 15-Minute Deep Check

```bash
# 4. Integration Tests
[integration-test-suite]

# 5. Critical Workflows (manual)
[your-app] [critical-workflow-1]
[your-app] [critical-workflow-2]

# 6. Performance Baseline
time [your-app] [performance-critical-operation]
```

## 🔧 Technology Quick Commands

| Tech        | Quality Check                               | Test            | Run                 |
| ----------- | ------------------------------------------- | --------------- | ------------------- |
| **Python**  | `ruff check && mypy && trunk check`         | `pytest`        | `python -m app`     |
| **Node.js** | `eslint . && tsc --noEmit`                  | `npm test`      | `node index.js`     |
| **Java**    | `mvn checkstyle:check && spotbugs:check`    | `mvn test`      | `java -jar app.jar` |
| **Go**      | `go vet && gofmt -d . && golangci-lint run` | `go test ./...` | `./binary`          |
| **Rust**    | `cargo clippy && rustfmt --check`           | `cargo test`    | `cargo run`         |

## 🚨 Common Refactor Bug Areas

- [ ] **Parameter passing** between functions/modules
- [ ] **File I/O paths** and directory creation
- [ ] **Configuration loading** and environment variables
- [ ] **External dependencies** (DB, APIs, services)
- [ ] **Error handling** and edge cases
- [ ] **Authentication/authorization** flows
- [ ] **Design patterns** (Factory, Builder, Decorator, Observer, Strategy)

## 🏗️ Quick Pattern Checks

- [ ] **Factory**: Returns interfaces, not concrete classes
- [ ] **Builder**: Method chaining works, `build()` validates
- [ ] **Decorator**: Interface compliance, proper delegation
- [ ] **Observer**: All observers notified on state change
- [ ] **Strategy**: Strategies interchangeable at runtime

## ✅ Success Criteria

- All tests pass
- No linting errors
- Critical workflows work
- Performance acceptable
- External integrations functional

## 🤖 AI Assistant Prompt (Quick Version)

```
I refactored [WHAT] in my [TECH_STACK] [PROJECT_TYPE].

Help me validate:
1. Run quality checks and tests for [TECH_STACK]
2. Test these critical workflows: [LIST_WORKFLOWS]
3. Check integration points: [LIST_DEPENDENCIES]
4. Verify no performance regressions

Focus on parameter passing and component connections where refactor bugs hide.
```

---

**💡 Key Insight**: Most refactor bugs occur at **integration points** where components connect, not within individual functions. Focus your testing there!
