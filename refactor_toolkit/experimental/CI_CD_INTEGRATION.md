# 🚀 CI/CD Integration Examples

**Complete CI/CD pipeline integration for the Universal Refactor Validation Toolkit**

This guide provides ready-to-use CI/CD configurations for GitHub Actions, GitLab CI, Azure DevOps, and Jenkins to integrate validation at multiple pipeline stages.

---

## 🎯 **Integration Strategy**

### **Multi-Stage Validation Approach**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PR/MR Check   │    │  Pre-Merge      │    │  Pre-Deploy     │    │  Post-Deploy    │
│   (5-10 min)    │    │  (15-30 min)    │    │  (30-45 min)    │    │  (5-10 min)     │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Quick validation│   │ • Pattern check │    │ • Full validation│   │ • Smoke tests   │
│ • Code quality   │    │ • Integration   │    │ • Security scan │    │ • Health check  │
│ • Unit tests     │    │ • Performance   │    │ • Load testing  │    │ • Metrics       │
│ • Mini assessment│    │ • Assessment    │    │ • Assessment    │    │ • Assessment    │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 🐙 **GitHub Actions**

### **Complete Workflow (.github/workflows/validation.yml)**

```yaml
name: Universal Refactor Validation

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [main]
  workflow_dispatch:

env:
  PYTHON_VERSION: "3.9"
  NODE_VERSION: "18"

jobs:
  # Stage 1: Quick Validation (PR Check)
  quick-validation:
    name: Quick Validation (5-10 min)
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'

    steps:
      - name: 📥 Checkout code
        uses: actions/checkout@v4

      - name: 🐍 Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: 📦 Install dependencies
        run: |
          pip install -r requirements.txt
          pip install ruff mypy pytest pytest-cov

      - name: ⚡ Run quick validation
        run: |
          chmod +x ./validate_quick.sh
          ./validate_quick.sh . auto

      - name: 📊 Upload quick assessment
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: quick-validation-report
          path: validation_quick_*.md

      - name: 💬 Comment PR with results
        uses: actions/github-script@v6
        if: always() && github.event_name == 'pull_request'
        with:
          script: |
            const fs = require('fs');
            const reportFiles = fs.readdirSync('.').filter(f => f.startsWith('validation_quick_'));
            if (reportFiles.length > 0) {
              const report = fs.readFileSync(reportFiles[0], 'utf8');
              github.rest.issues.createComment({
                issue_number: context.issue.number,
                owner: context.repo.owner,
                repo: context.repo.repo,
                body: `## 🔍 Quick Validation Results\n\n\`\`\`markdown\n${report}\n\`\`\``
              });
            }

  # Stage 2: Pattern & Integration Validation
  pattern-validation:
    name: Pattern & Integration (15-30 min)
    runs-on: ubuntu-latest
    needs: quick-validation
    if: github.event_name == 'pull_request'

    steps:
      - name: 📥 Checkout code
        uses: actions/checkout@v4

      - name: 🐍 Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: 📦 Install validation tools
        run: |
          pip install -r requirements.txt
          pip install radon bandit safety

      - name: 🏗️ Run pattern validation
        run: |
          python validate_patterns.py ./src --auto-detect --verbose

      - name: 🔗 Run integration tests
        run: |
          pytest tests/integration/ -v --cov=src --cov-report=xml

      - name: 📊 Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml

      - name: 🔒 Security scan
        run: |
          bandit -r ./src -f json -o bandit-report.json || true
          safety check --json --output safety-report.json || true

      - name: 📊 Upload security reports
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: security-reports
          path: |
            bandit-report.json
            safety-report.json

  # Stage 3: Comprehensive Validation (Pre-Deploy)
  comprehensive-validation:
    name: Comprehensive Validation (30-45 min)
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'

    steps:
      - name: 📥 Checkout code
        uses: actions/checkout@v4

      - name: 🐍 Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: 📦 Install all dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt

      - name: 🏗️ Run comprehensive validation
        run: |
          chmod +x ./validate_full.sh
          ./validate_full.sh . auto

      - name: 📊 Upload comprehensive report
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: comprehensive-validation-report
          path: validation_comprehensive_*.md

      - name: 🚨 Fail on critical issues
        run: |
          # Check if validation passed (exit code 0)
          if [ $? -ne 0 ]; then
            echo "❌ Comprehensive validation failed - blocking deployment"
            exit 1
          fi

      - name: ✅ Mark as deployment ready
        run: |
          echo "✅ All validation layers passed - ready for deployment"
          echo "DEPLOYMENT_READY=true" >> $GITHUB_ENV

  # Stage 4: Post-Deploy Validation
  post-deploy-validation:
    name: Post-Deploy Smoke Tests (5-10 min)
    runs-on: ubuntu-latest
    needs: comprehensive-validation
    if: success() && github.ref == 'refs/heads/main'

    steps:
      - name: 📥 Checkout code
        uses: actions/checkout@v4

      - name: 🔍 Run smoke tests
        run: |
          # Add your smoke test commands here
          python -m pytest tests/smoke/ -v

      - name: 📊 Health check
        run: |
          # Add health check commands
          curl -f http://your-app-url/health || exit 1

      - name: 📈 Collect metrics
        run: |
          # Add metrics collection
          echo "Deployment validation complete"
```

### **Pre-commit Hook Integration (.github/workflows/pre-commit.yml)**

```yaml
name: Pre-commit Validation

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  pre-commit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: "3.9"
      - name: Install pre-commit
        run: pip install pre-commit
      - name: Run pre-commit
        run: pre-commit run --all-files
      - name: Run pattern validation
        run: python validate_patterns.py ./src --auto-detect
```

---

## 🦊 **GitLab CI**

### **Complete Pipeline (.gitlab-ci.yml)**

```yaml
stages:
  - quick-validation
  - pattern-validation
  - comprehensive-validation
  - deploy
  - post-deploy

variables:
  PYTHON_VERSION: "3.9"
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"

cache:
  paths:
    - .cache/pip
    - venv/

# Stage 1: Quick Validation
quick-validation:
  stage: quick-validation
  image: python:${PYTHON_VERSION}
  script:
    - pip install -r requirements.txt
    - pip install ruff mypy pytest pytest-cov
    - chmod +x ./validate_quick.sh
    - ./validate_quick.sh . auto
  artifacts:
    reports:
      junit: test-results.xml
    paths:
      - validation_quick_*.md
    expire_in: 1 week
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"

# Stage 2: Pattern Validation
pattern-validation:
  stage: pattern-validation
  image: python:${PYTHON_VERSION}
  script:
    - pip install -r requirements.txt
    - pip install radon bandit safety
    - python validate_patterns.py ./src --auto-detect --verbose
    - pytest tests/integration/ -v --cov=src --cov-report=xml --junit-xml=test-results.xml
    - bandit -r ./src -f json -o bandit-report.json || true
  artifacts:
    reports:
      junit: test-results.xml
      coverage_report:
        coverage_format: cobertura
        path: coverage.xml
    paths:
      - bandit-report.json
    expire_in: 1 week
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"

# Stage 3: Comprehensive Validation
comprehensive-validation:
  stage: comprehensive-validation
  image: python:${PYTHON_VERSION}
  script:
    - pip install -r requirements.txt
    - pip install -r requirements-dev.txt
    - chmod +x ./validate_full.sh
    - ./validate_full.sh . auto
  artifacts:
    paths:
      - validation_comprehensive_*.md
    expire_in: 1 month
  rules:
    - if: $CI_COMMIT_BRANCH == "main"

# Stage 4: Deploy (only after successful validation)
deploy:
  stage: deploy
  script:
    - echo "Deploying application..."
    -  # Add your deployment commands here
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
  needs:
    - comprehensive-validation

# Stage 5: Post-Deploy Validation
post-deploy-validation:
  stage: post-deploy
  script:
    - python -m pytest tests/smoke/ -v
    - curl -f $APP_URL/health
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
  needs:
    - deploy
```

---

## 🔵 **Azure DevOps**

### **Complete Pipeline (azure-pipelines.yml)**

```yaml
trigger:
  branches:
    include:
      - main
      - develop

pr:
  branches:
    include:
      - main

pool:
  vmImage: "ubuntu-latest"

variables:
  pythonVersion: "3.9"

stages:
  - stage: QuickValidation
    displayName: "Quick Validation (5-10 min)"
    condition: eq(variables['Build.Reason'], 'PullRequest')
    jobs:
      - job: QuickValidation
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "$(pythonVersion)"
            displayName: "Use Python $(pythonVersion)"

          - script: |
              pip install -r requirements.txt
              pip install ruff mypy pytest pytest-cov
            displayName: "Install dependencies"

          - script: |
              chmod +x ./validate_quick.sh
              ./validate_quick.sh . auto
            displayName: "Run quick validation"

          - task: PublishTestResults@2
            condition: succeededOrFailed()
            inputs:
              testResultsFiles: "test-results.xml"
              testRunTitle: "Quick Validation Tests"

          - task: PublishBuildArtifacts@1
            condition: always()
            inputs:
              pathToPublish: "validation_quick_*.md"
              artifactName: "quick-validation-report"

  - stage: PatternValidation
    displayName: "Pattern & Integration (15-30 min)"
    condition: eq(variables['Build.Reason'], 'PullRequest')
    dependsOn: QuickValidation
    jobs:
      - job: PatternValidation
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "$(pythonVersion)"

          - script: |
              pip install -r requirements.txt
              pip install radon bandit safety
            displayName: "Install validation tools"

          - script: |
              python validate_patterns.py ./src --auto-detect --verbose
            displayName: "Run pattern validation"

          - script: |
              pytest tests/integration/ -v --cov=src --cov-report=xml --junit-xml=test-results.xml
            displayName: "Run integration tests"

          - task: PublishCodeCoverageResults@1
            inputs:
              codeCoverageTool: Cobertura
              summaryFileLocation: "coverage.xml"

  - stage: ComprehensiveValidation
    displayName: "Comprehensive Validation (30-45 min)"
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
    jobs:
      - job: ComprehensiveValidation
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "$(pythonVersion)"

          - script: |
              pip install -r requirements.txt
              pip install -r requirements-dev.txt
            displayName: "Install all dependencies"

          - script: |
              chmod +x ./validate_full.sh
              ./validate_full.sh . auto
            displayName: "Run comprehensive validation"

          - task: PublishBuildArtifacts@1
            condition: always()
            inputs:
              pathToPublish: "validation_comprehensive_*.md"
              artifactName: "comprehensive-validation-report"

  - stage: Deploy
    displayName: "Deploy"
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    dependsOn: ComprehensiveValidation
    jobs:
      - deployment: Deploy
        environment: "production"
        strategy:
          runOnce:
            deploy:
              steps:
                - script: echo "Deploying application..."
                  displayName: "Deploy application"

  - stage: PostDeployValidation
    displayName: "Post-Deploy Validation (5-10 min)"
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    dependsOn: Deploy
    jobs:
      - job: PostDeployValidation
        steps:
          - script: |
              python -m pytest tests/smoke/ -v
            displayName: "Run smoke tests"

          - script: |
              curl -f $(APP_URL)/health
            displayName: "Health check"
```

---

## 🔨 **Jenkins**

### **Declarative Pipeline (Jenkinsfile)**

```groovy
pipeline {
    agent any

    environment {
        PYTHON_VERSION = '3.9'
        VENV_PATH = 'venv'
    }

    stages {
        stage('Setup') {
            steps {
                script {
                    // Create virtual environment
                    sh """
                        python${PYTHON_VERSION} -m venv ${VENV_PATH}
                        . ${VENV_PATH}/bin/activate
                        pip install --upgrade pip
                        pip install -r requirements.txt
                    """
                }
            }
        }

        stage('Quick Validation') {
            when {
                changeRequest()
            }
            steps {
                script {
                    sh """
                        . ${VENV_PATH}/bin/activate
                        pip install ruff mypy pytest pytest-cov
                        chmod +x ./validate_quick.sh
                        ./validate_quick.sh . auto
                    """
                }
            }
            post {
                always {
                    archiveArtifacts artifacts: 'validation_quick_*.md', allowEmptyArchive: true
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: '.',
                        reportFiles: 'validation_quick_*.md',
                        reportName: 'Quick Validation Report'
                    ])
                }
            }
        }

        stage('Pattern Validation') {
            when {
                changeRequest()
            }
            steps {
                script {
                    sh """
                        . ${VENV_PATH}/bin/activate
                        pip install radon bandit safety
                        python validate_patterns.py ./src --auto-detect --verbose
                        pytest tests/integration/ -v --cov=src --cov-report=xml --junit-xml=test-results.xml
                        bandit -r ./src -f json -o bandit-report.json || true
                    """
                }
            }
            post {
                always {
                    junit 'test-results.xml'
                    publishCoverage adapters: [coberturaAdapter('coverage.xml')], sourceFileResolver: sourceFiles('STORE_LAST_BUILD')
                    archiveArtifacts artifacts: 'bandit-report.json', allowEmptyArchive: true
                }
            }
        }

        stage('Comprehensive Validation') {
            when {
                branch 'main'
            }
            steps {
                script {
                    sh """
                        . ${VENV_PATH}/bin/activate
                        pip install -r requirements-dev.txt
                        chmod +x ./validate_full.sh
                        ./validate_full.sh . auto
                    """
                }
            }
            post {
                always {
                    archiveArtifacts artifacts: 'validation_comprehensive_*.md', allowEmptyArchive: true
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: '.',
                        reportFiles: 'validation_comprehensive_*.md',
                        reportName: 'Comprehensive Validation Report'
                    ])
                }
                failure {
                    emailext (
                        subject: "❌ Comprehensive Validation Failed: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                        body: "Comprehensive validation failed. Check the build logs and validation report.",
                        to: "${env.CHANGE_AUTHOR_EMAIL}"
                    )
                }
            }
        }

        stage('Deploy') {
            when {
                allOf {
                    branch 'main'
                    expression { currentBuild.result == null || currentBuild.result == 'SUCCESS' }
                }
            }
            steps {
                script {
                    echo "Deploying application..."
                    // Add your deployment steps here
                }
            }
        }

        stage('Post-Deploy Validation') {
            when {
                allOf {
                    branch 'main'
                    expression { currentBuild.result == null || currentBuild.result == 'SUCCESS' }
                }
            }
            steps {
                script {
                    sh """
                        . ${VENV_PATH}/bin/activate
                        python -m pytest tests/smoke/ -v
                        curl -f \${APP_URL}/health
                    """
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        success {
            echo "✅ All validation stages completed successfully"
        }
        failure {
            echo "❌ Validation pipeline failed"
        }
    }
}
```

---

## 🔧 **Pre-commit Hooks Configuration**

### **.pre-commit-config.yaml**

```yaml
repos:
  # Code Quality
  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.9

  - repo: https://github.com/charliermarsh/ruff-pre-commit
    rev: v0.0.287
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.5.1
    hooks:
      - id: mypy
        additional_dependencies: [types-requests]

  # Security
  - repo: https://github.com/PyCQA/bandit
    rev: 1.7.5
    hooks:
      - id: bandit
        args: ["-c", "pyproject.toml"]

  # Pattern Validation (Custom)
  - repo: local
    hooks:
      - id: pattern-validation
        name: Design Pattern Validation
        entry: python validate_patterns.py
        language: system
        args: [".", "--auto-detect"]
        pass_filenames: false

  # Quick Validation (Custom)
  - repo: local
    hooks:
      - id: quick-validation
        name: Quick Validation Check
        entry: ./validate_quick.sh
        language: system
        args: [".", "auto"]
        pass_filenames: false
        stages: [manual] # Only run when explicitly called
```

---

## 📊 **Integration Best Practices**

### **1. Stage Gating Strategy**

```yaml
# Example: Require validation success before merge
branch_protection_rules:
  main:
    required_status_checks:
      - "Quick Validation (5-10 min)"
      - "Pattern & Integration (15-30 min)"
    required_reviews: 1
    dismiss_stale_reviews: true
```

### **2. Parallel Execution**

```yaml
# Run independent validations in parallel
jobs:
  code-quality:
    runs-on: ubuntu-latest
    steps: [...]

  unit-tests:
    runs-on: ubuntu-latest
    steps: [...]

  pattern-validation:
    runs-on: ubuntu-latest
    steps: [...]

  security-scan:
    runs-on: ubuntu-latest
    steps: [...]

  integration-tests:
    runs-on: ubuntu-latest
    needs: [code-quality, unit-tests] # Only after basic checks pass
    steps: [...]
```

### **3. Conditional Execution**

```yaml
# Run expensive validations only when needed
comprehensive-validation:
  if: |
    github.event_name == 'push' &&
    github.ref == 'refs/heads/main' &&
    contains(github.event.head_commit.message, '[full-validation]')
```

### **4. Artifact Management**

```yaml
# Preserve validation reports
- name: Upload validation artifacts
  uses: actions/upload-artifact@v3
  if: always()
  with:
    name: validation-reports-${{ github.run_id }}
    path: |
      validation_*.md
      *-report.json
    retention-days: 30
```

### **5. Notification Strategy**

```yaml
# Smart notifications based on validation results
- name: Notify on validation failure
  if: failure()
  uses: 8398a7/action-slack@v3
  with:
    status: failure
    text: |
      🔴 Validation failed for ${{ github.repository }}
      📊 Check validation report: ${{ steps.upload.outputs.artifact-url }}
      🔗 Commit: ${{ github.event.head_commit.message }}
```

---

## 🎯 **Customization Guide**

### **Technology-Specific Adaptations**

**For JavaScript/Node.js:**

```yaml
- name: Install Node.js dependencies
  run: npm ci
- name: Run validation
  run: |
    npm run lint
    npm test
    npm run build
    ./validate_quick.sh . javascript
```

**For Java:**

```yaml
- name: Setup Java
  uses: actions/setup-java@v3
  with:
    java-version: "11"
- name: Run validation
  run: |
    mvn clean compile
    mvn test
    ./validate_quick.sh . java
```

**For Go:**

```yaml
- name: Setup Go
  uses: actions/setup-go@v4
  with:
    go-version: "1.21"
- name: Run validation
  run: |
    go fmt ./...
    go vet ./...
    go test ./...
    ./validate_quick.sh . go
```

### **Environment-Specific Configurations**

**Development Environment:**

- Run quick validation only
- Allow some test failures
- Generate mini assessments

**Staging Environment:**

- Run comprehensive validation
- Strict quality gates
- Full assessment reports

**Production Environment:**

- Run all validation layers
- Zero tolerance for failures
- Detailed audit trails

---

**These CI/CD integrations ensure consistent, automated validation across your entire development lifecycle while maintaining fast feedback loops and comprehensive quality assurance.**
