# Migration Configuration System

The schema-diff migration analysis system supports configurable domain-specific intelligence through a flexible configuration system. This allows you to customize field pattern recognition, compatibility thresholds, and report templates for different industries and use cases.

## Quick Start

### Using Predefined Domains

```bash
# LinkedIn data (default)
schema-diff old.json new.json --migration-analysis report.md

# E-commerce domain
schema-diff products_old.json products_new.json --migration-analysis report.md --migration-domain ecommerce

# Financial services domain
schema-diff transactions_old.json transactions_new.json --migration-analysis report.md --migration-domain financial

# Generic domain (no domain-specific patterns)
schema-diff data1.json data2.json --migration-analysis report.md --migration-domain generic
```

### Using Custom Configuration Files

```bash
# YAML configuration
schema-diff old.json new.json --migration-analysis report.md --migration-config my_config.yaml

# JSON configuration
schema-diff old.json new.json --migration-analysis report.md --migration-config my_config.json
```

## Configuration Structure

### Domain Information

```yaml
domain_name: "Your Domain Name" # Required: Display name for reports
source_system: "Legacy System Name" # Optional: Source system description
target_system: "New System Name" # Optional: Target system description
```

### Field Patterns

Field patterns help the analyzer categorize fields and identify critical issues:

```yaml
field_patterns:
  # Generic patterns (applied to all domains)
  critical_patterns:
    - "id"
    - "key"
    - "uuid"
    - "primary_key"
    - "foreign_key"
    - "created_at"
    - "updated_at"

  # Domain-specific critical patterns
  domain_critical_patterns:
    - "customer_id"
    - "order_id"
    - "product_id"
    - "transaction_id"

  # Generic audit field patterns
  audit_patterns:
    - "created_at"
    - "updated_at"
    - "deleted"
    - "deleted_at"
    - "version"
    - "audit"
    - "metadata"
    - "tracking"

  # Domain-specific audit patterns
  domain_audit_patterns:
    - "order_date"
    - "payment_status"
    - "inventory_updated"
    - "compliance_check"
```

### Compatibility Thresholds

Control how compatibility is assessed based on common field counts:

```yaml
thresholds:
  excellent_threshold: 150 # "Excellent" compatibility rating
  good_threshold: 75 # "Good" compatibility rating
  audit_field_threshold: 30 # Threshold for "audit field explosion" warning
```

**Compatibility Ratings:**

- `excellent`: ≥ excellent_threshold common fields
- `good`: ≥ good_threshold but < excellent_threshold common fields
- `limited`: < good_threshold common fields

### Report Templates

Customize the content and tone of migration reports:

```yaml
templates:
  # Template for critical issue descriptions
  critical_issue_template: |
    **Impact**: Critical for e-commerce operations - customer orders may be affected
    **Action**: Immediate review required before migration
    **Business Risk**: High - potential revenue impact

  # Generic next steps for all migrations
  generic_next_steps:
    - "Validate customer and order data integrity"
    - "Test payment processing with new schema"
    - "Verify inventory tracking accuracy"
    - "Choose migration strategy"
    - "Plan for minimal downtime during migration"

  # Templates for specific warning types
  warning_templates:
    audit_field_explosion: |
      **Impact**: Significant increase in e-commerce data complexity
      **Details**: New audit fields added for better order tracking
      **Business Value**: Improved customer service capabilities
      **Recommendations**:
      - Keep audit fields for better customer support
      - Use for fraud detection and order tracking
      - Consider storage costs vs business benefits

    field_location_changes: |
      **Impact**: E-commerce workflows may need updates
      **Details**: Product or order fields moved to new locations
      **Action required**: Update checkout and inventory systems
      **Recommendations**:
      - Test checkout flow thoroughly
      - Verify inventory management integration
      - Update customer service tools

    fields_removed: |
      **Impact**: Potential loss of customer or order history
      **Details**: Fields from legacy system not in new platform
      **Action required**: Assess customer impact
      **Recommendations**:
      - Archive historical customer data
      - Update customer service procedures
      - Check regulatory compliance requirements

    new_audit_fields: |
      **Impact**: New metadata fields for tracking
      **Details**: Standard audit fields for data governance
      **Recommendations**:
      - Evaluate business value of new fields
      - Consider selective inclusion
      - May improve debugging capabilities
```

## Predefined Configurations

### LinkedIn Domain

Optimized for LinkedIn member data migrations:

- **Critical patterns**: `member_id`, `user_id`, `email`, `name`
- **Audit patterns**: Standard audit fields
- **Thresholds**: 100 excellent, 50 good, 50 audit explosion
- **Templates**: Generic business-focused guidance

### E-commerce Domain

Optimized for e-commerce platform migrations:

- **Critical patterns**: `customer_id`, `order_id`, `product_id`, `transaction_id`, `sku`, `payment_id`
- **Audit patterns**: `order_date`, `payment_status`, `inventory_updated`, `price_updated`
- **Thresholds**: 150 excellent, 75 good, 30 audit explosion
- **Templates**: E-commerce specific impact assessments and recommendations

### Financial Domain

Optimized for banking and financial services:

- **Critical patterns**: `account_id`, `transaction_id`, `routing_number`, `account_number`, `ssn`
- **Audit patterns**: `compliance_check`, `risk_score`, `fraud_check`, `kyc_status`, `aml_status`
- **Thresholds**: 200 excellent, 100 good, 25 audit explosion
- **Templates**: Compliance-focused with regulatory considerations

### Generic Domain

Minimal configuration with no domain-specific patterns:

- **Critical patterns**: Basic ID and timestamp fields only
- **Audit patterns**: Standard audit fields only
- **Thresholds**: 100 excellent, 50 good, 50 audit explosion
- **Templates**: Generic guidance without industry specifics

## Configuration Examples

### Minimal Configuration

```yaml
domain_name: "My Custom Domain"
```

This inherits all defaults but customizes the domain name in reports.

### E-commerce Configuration

```yaml
domain_name: "E-commerce Platform"
source_system: "Legacy WooCommerce"
target_system: "Modern Shopify Plus"

field_patterns:
  domain_critical_patterns:
    - "customer_id"
    - "order_id"
    - "product_id"
    - "sku"
    - "payment_id"

  domain_audit_patterns:
    - "order_date"
    - "payment_status"
    - "inventory_updated"

thresholds:
  excellent_threshold: 150
  good_threshold: 75
  audit_field_threshold: 30

templates:
  generic_next_steps:
    - "Validate customer and order data integrity"
    - "Test payment processing with new schema"
    - "Verify inventory tracking accuracy"
    - "Choose migration strategy"
    - "Plan for minimal downtime during migration"
```

### Healthcare Configuration

```yaml
domain_name: "Healthcare System"
source_system: "Legacy EMR"
target_system: "Modern FHIR-compliant System"

field_patterns:
  domain_critical_patterns:
    - "patient_id"
    - "medical_record_number"
    - "provider_id"
    - "encounter_id"
    - "diagnosis_code"
    - "procedure_code"

  domain_audit_patterns:
    - "admission_date"
    - "discharge_date"
    - "last_updated_by"
    - "hipaa_log"
    - "access_log"

thresholds:
  excellent_threshold: 300
  good_threshold: 150
  audit_field_threshold: 20

templates:
  critical_issue_template: |
    **Impact**: CRITICAL - Patient data integrity at risk
    **Compliance Risk**: High - may violate HIPAA regulations
    **Action**: Immediate review with compliance team required
    **Patient Safety**: Ensure continuity of care during migration

  generic_next_steps:
    - "Validate patient data integrity and completeness"
    - "Ensure HIPAA compliance throughout migration"
    - "Test clinical workflows with new schema"
    - "Verify audit trail completeness"
    - "Coordinate with clinical and compliance teams"
    - "Plan for zero-downtime migration"
```

## Validation

The configuration system includes comprehensive validation:

### Automatic Validation

All configurations are automatically validated when loaded:

```bash
❌ Configuration validation failed:
Configuration validation failed:
- domain_name must be a non-empty string
- thresholds.excellent_threshold must be a non-negative integer
- thresholds.excellent_threshold must be greater than good_threshold
- templates.warning_templates must include 'audit_field_explosion' template

💡 Please fix the configuration file and try again.
```

### Validation Rules

1. **Domain Information**:
   - `domain_name` must be a non-empty string
   - `source_system` and `target_system` must be non-empty strings if provided

2. **Field Patterns**:
   - All pattern arrays must contain only strings
   - Pattern strings must be non-empty

3. **Thresholds**:
   - All thresholds must be non-negative integers
   - `excellent_threshold` must be greater than `good_threshold`

4. **Templates**:
   - `critical_issue_template` must be a non-empty string
   - `generic_next_steps` must be an array of non-empty strings
   - `warning_templates` must include all required templates:
     - `audit_field_explosion`
     - `field_location_changes`
     - `fields_removed`
     - `new_audit_fields`

## JSON Schema

A complete JSON Schema is available at `examples/migration_configs/config_schema.json` for IDE validation and documentation.

## Best Practices

### 1. Start with Predefined Domains

Use predefined domains when they match your use case:

```bash
schema-diff old.json new.json --migration-analysis report.md --migration-domain ecommerce
```

### 2. Customize Incrementally

Start with a minimal configuration and add customizations:

```yaml
# Start simple
domain_name: "My Domain"

# Add domain-specific patterns
field_patterns:
  domain_critical_patterns:
    - "my_critical_field"
```

### 3. Version Control Configurations

Keep configuration files in version control alongside your schemas:

```
project/
├── schemas/
│   ├── old_schema.json
│   └── new_schema.json
├── migration_configs/
│   └── production_config.yaml
└── migrations/
    └── analysis_report.md
```

### 4. Test Configurations

Validate configurations with sample data before using in production:

```bash
# Test with small sample
schema-diff sample_old.json sample_new.json --migration-config test_config.yaml --migration-analysis test_report.md --samples 10
```

### 5. Document Custom Patterns

Document why specific patterns were chosen:

```yaml
field_patterns:
  domain_critical_patterns:
    # Core business identifiers - losing these breaks customer lookup
    - "customer_id"
    - "account_number"
    # Payment processing - required for transaction integrity
    - "payment_id"
    - "transaction_id"
```

### 6. Adjust Thresholds for Schema Size

Larger schemas need higher thresholds:

```yaml
thresholds:
  # For large schemas (500+ fields)
  excellent_threshold: 400
  good_threshold: 200

  # For small schemas (50 fields)
  excellent_threshold: 40
  good_threshold: 20
```

### 7. Customize Templates for Audience

Tailor templates for your audience:

```yaml
templates:
  # For technical teams
  critical_issue_template: |
    **Impact**: Database constraint violations likely
    **Action**: Update foreign key mappings

  # For business stakeholders
  critical_issue_template: |
    **Impact**: Customer data may be lost during migration
    **Action**: Business review required before proceeding
```

## Troubleshooting

### Configuration Not Loading

1. Check file path and permissions
2. Validate YAML/JSON syntax
3. Review validation error messages

### Unexpected Compatibility Ratings

1. Check threshold values match your schema size
2. Verify field patterns are matching expected fields
3. Use `--show-common` to see what fields are being counted

### Missing Warning Templates

Ensure all required warning templates are defined:

```yaml
templates:
  warning_templates:
    audit_field_explosion: "..."
    field_location_changes: "..."
    fields_removed: "..."
    new_audit_fields: "..."
```

### Pattern Matching Issues

Field patterns use substring matching. Be specific:

```yaml
# Too broad - matches "user_id_hash", "pseudo_id", etc.
domain_critical_patterns: ["id"]

# Better - more specific
domain_critical_patterns: ["user_id", "customer_id", "order_id"]
```
