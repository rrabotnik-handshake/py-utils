#!/usr/bin/env python3
"""BigQuery DDL and anti-pattern detection configuration.

This module centralizes all configuration constants, thresholds, catalogs,
and SQL templates used in BigQuery schema analysis and DDL generation.

Organization:
- Security & PII Detection
- Anti-Pattern Detection Thresholds
- Naming Conventions & Patterns
- DDL Generation Configuration
- SQL Query Templates

All values are designed to be easily customizable per organization.
"""
from __future__ import annotations

import os

# =============================================================================
# SECURITY & PII DETECTION
# =============================================================================

# PII indicators organized by category
PII_INDICATORS: dict[str, set[str]] = {
    # Contact
    "contact": {
        "email",
        "phone",
        "mobile",
        "fax",
        "telephone",
    },
    # Names
    "name": {
        "first_name",
        "last_name",
        "full_name",
        "middle_name",
        "maiden_name",
        "given_name",
        "family_name",
        "nickname",
        "display_name",
        "legal_name",
        "preferred_name",
    },
    # Government IDs
    "gov_id": {
        "ssn",
        "social_security",
        "passport",
        "license",
        "driver_license",
        "national_id",
        "tax_id",
        "ein",
        "tin",
        "citizen_id",
        "resident_id",
    },
    # Financial
    "financial": {
        "salary",
        "wage",
        "income",
        "compensation",
        "credit_card",
        "bank_account",
        "routing_number",
        "iban",
        "swift",
        "account_number",
    },
    # Personal dates
    "personal_date": {
        "birth_date",
        "birthdate",
        "dob",
        "date_of_birth",
    },
    # Location
    "location": {
        "address",
        "street",
        "home_address",
        "residence",
        "location",
        "zip_code",
        "postal_code",
        "postcode",
    },
    # Medical
    "medical": {
        "medical",
        "health",
        "diagnosis",
        "prescription",
        "patient",
        "hipaa",
    },
    # Biometric / imagery
    "biometric": {
        "fingerprint",
        "retina",
        "facial",
        "biometric",
        "photo",
        "image",
        "picture",
    },
    # Other device/user identifiers
    "device_user_id": {
        "ip_address",
        "mac_address",
        "device_id",
        "imei",
        "uuid",
        "username",
        "login",
    },
}

# Sensitive secrets (exact match)
SENSITIVE_SECRETS_EXACT: set[str] = {
    "password",
    "pwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "access_token",
    "refresh_token",
    "auth_token",
    "session_token",
    "bearer_token",
}

# Sensitive secret suffixes (for *_token, *_secret detection)
SENSITIVE_SECRET_SUFFIXES = frozenset(
    {
        "_secret",
        "_token",
        "_private_key",
    }
)

# PII detection exclusions (to reduce false positives)
PII_EXCLUDE_EXACT = {"id", "uuid", "guid", "key", "index", "order", "position", "rank"}
PII_EXCLUDE_SUFFIXES = ("_id", "_uuid", "_guid", "_key", "_ref", "_index")

# Optional: enforce policy tags on PII (set in CI)
REQUIRE_POLICY_TAGS_FOR_PII = (
    os.environ.get("REQUIRE_POLICY_TAGS_FOR_PII", "").lower() == "true"
)

# Optional: restrict to specific taxonomy prefix
REQUIRED_PII_TAXONOMY_PREFIX = os.environ.get("REQUIRED_PII_TAXONOMY_PREFIX", "")


# =============================================================================
# DIMENSIONAL MODELING DETECTION
# =============================================================================

# Star/Snowflake detection
HUB_FK_THRESHOLD = 5  # Minimum inbound FKs to be considered a fact hub
SNOWFLAKE_DIM_PREFIX = "dim_"  # Prefix for dimension tables

# Fact table grain detection
GRAIN_AMBIGUOUS_DATE_THRESHOLD = 2  # Multiple date keys suggest ambiguous grain
FACT_SURROGATE_KEY_NAMES = frozenset(
    {
        "fact_id",
        "row_id",
        "id",
        "pk",
        "surrogate_key",
        "_surrogate_key",  # dbt standard
        "sk",
    }
)

# Fact surrogate key suffixes (for contains/endswith checks)
FACT_SURROGATE_KEY_SUFFIXES = frozenset(
    {
        "_id",
        "_key",
        "_sk",
        "_surrogate_key",  # dbt standard
    }
)

# Measure classification
ADDITIVE_MEASURE_INDICATORS = frozenset(
    {
        "amount",
        "price",
        "cost",
        "fee",
        "revenue",
        "qty",
        "quantity",
        "count",
        "units",
        "seconds",
        "minutes",
        "hours",
        "total",
        "sum",
    }
)

SEMI_ADDITIVE_MEASURE_INDICATORS = frozenset(
    {
        "balance",
        "snapshot",
        "on_hand",
        "end_qty",
        "inventory",
        "outstanding",
        "pending",
    }
)

NON_ADDITIVE_MEASURE_INDICATORS = frozenset(
    {
        "rate",
        "ratio",
        "pct",
        "percent",
        "percentage",
        "avg",
        "average",
        "median",
        "distinct",
        "margin",
    }
)

# Date/Time dimensions
DATE_DIMENSION_NAMES = frozenset(
    {
        "dim_date",
        "dim_time",
        "date_dim",
        "time_dim",
    }
)

ROLE_PLAYING_DATE_SUFFIXES = frozenset(
    {
        "_date",
        "_dt",
        "_timestamp",
        "_ts",
        "_time",
    }
)

# Slowly Changing Dimensions (SCD)
SCD_TYPE2_INDICATORS = frozenset(
    {
        "effective_start",
        "effective_end",
        "valid_from",
        "valid_to",
        "_valid_from",  # dbt standard
        "_valid_to",  # dbt standard
        "start_date",
        "end_date",
        "is_current",
        "is_active",
        "current_flag",
        "active_flag",
    }
)

# Junk dimension detection
JUNK_DIM_FLAG_THRESHOLD = 4  # 4+ low-cardinality flags → junk dim candidate
JUNK_DIM_INDICATORS = frozenset(
    {
        "status",
        "type",
        "flag",
        "indicator",
        "code",
    }
)

# Bridge table patterns
BRIDGE_TABLE_PREFIX = "bridge_"
BRIDGE_TABLE_SUFFIXES = frozenset(
    {
        "_bridge",
        "_xref",
        "_mapping",
    }
)

# Fact types
TRANSACTION_FACT_INDICATORS = frozenset(
    {
        "transaction",
        "event",
        "order",
        "sale",
        "payment",
    }
)

SNAPSHOT_FACT_INDICATORS = frozenset(
    {
        "snapshot",
        "daily",
        "weekly",
        "monthly",
        "period",
    }
)

ACCUMULATING_SNAPSHOT_INDICATORS = frozenset(
    {
        "milestone",
        "accumulating",
        "lifecycle",
    }
)

# Unknown/Not Applicable member keys
UNKNOWN_MEMBER_KEY = -1
NOT_APPLICABLE_KEY = 0

# Nested vs Flat modeling
LINE_ITEM_ARRAY_NAMES = frozenset(
    {
        "items",
        "line_items",
        "order_items",
        "details",
        "lines",
    }
)


# =============================================================================
# ANTI-PATTERN DETECTION THRESHOLDS
# =============================================================================

# --- Nesting & Complexity ---
MAX_NESTING_DEPTH = 10  # Flag if depth exceeds this
WARN_NESTING_DEPTH = 5  # Warn if multiple fields exceed this
WARN_NESTING_DEPTH_THRESHOLD = 8  # Severity threshold for max depth

# --- Table Size Thresholds ---
MAX_TOP_LEVEL_FIELDS = 100  # Error threshold
WARN_TOP_LEVEL_FIELDS = 50  # Warning threshold
GOD_TABLE_THRESHOLD = 80  # Too many unrelated fields

# --- Naming Conventions ---
MIN_ACCEPTABLE_NAME_LENGTH = 2  # Names shorter than this are cryptic
SHORT_NAME_LENGTH = 6  # Names this length or less are checked for abbreviations
LONG_NAME_LENGTH = 50  # Names longer than this are flagged

# --- Structural Thresholds ---
COMPLEX_STRUCT_FIELD_COUNT = 5  # Flag RECORD with > this many fields as complex
MIN_STRUCT_FIELD_COUNT_FOR_ORDER = (
    1  # Min fields to require ordering in REPEATED RECORD
)
ARRAY_NEEDS_ID_FIELD_COUNT = 2  # Arrays with > this many fields should have ID
OVER_STRUCT_MAX_FIELDS = 2  # Structs with <= this many fields might be over-structured

# --- Consistency Checks ---
NAMING_INCONSISTENCY_THRESHOLD = 0.2  # >20% inconsistency triggers warning
CASING_MINORITY_THRESHOLD = 0.15  # >15% minority casing triggers warning
MIN_FIELDS_FOR_CONSISTENCY_CHECK = 5  # Need this many fields to check consistency
DUPLICATE_SIGNATURE_MIN_SIZE = 6  # Signature must be this long for duplicate check
DUPLICATE_SIGNATURE_MIN_COUNT = 3  # Need >= this many duplicates to flag
PREFIX_REDUNDANCY_THRESHOLD = 0.5  # >50% of fields share prefix
PREFIX_MIN_OCCURRENCE = 4  # Prefix must appear >= this many times


# =============================================================================
# NAMING PATTERNS & FIELD INDICATORS
# =============================================================================

# --- Acceptable Short Names/Abbreviations ---
ACCEPTABLE_SHORT_NAMES = {
    "id",
    "at",
    "by",
    "to",
    "ts",
    "no",
    "ip",
    "os",
    "db",
    "pk",
    "fk",
    "url",
    "uri",
    "api",
    "ui",
    "ux",
    "qa",
    "ci",
    "cd",
    "etl",
    "ssn",
    "ein",
    "vat",
    "gst",
    "sku",
    "upc",
    "isbn",
}

ACCEPTABLE_ABBREVIATIONS = {
    # Identifiers
    "id",
    "uid",
    "uuid",
    "guid",
    # Network/protocols
    "url",
    "uri",
    "api",
    "ip",
    "mac",
    "http",
    "https",
    "ftp",
    "ssh",
    "ssl",
    "tls",
    # Systems/hardware
    "os",
    "cpu",
    "gpu",
    "ram",
    "ssd",
    "db",
    "pk",
    "fk",
    # File formats
    "pdf",
    "jpg",
    "png",
    "gif",
    "svg",
    "html",
    "css",
    "js",
    # Statistics/math
    "min",
    "max",
    "avg",
    "sum",
    "std",
    "var",
    # Business/accounting
    "ssn",
    "ein",
    "vat",
    "gst",
    "sku",
    "upc",
    "isbn",
    # Time/temporal
    "at",
    "by",
    "to",
    "ts",
    "no",
    # User experience
    "ui",
    "ux",
    "qa",
    "ci",
    "cd",
    "etl",
}

# --- Type-Specific Field Patterns ---
JSON_BLOB_FIELDS = {
    "json",
    "blob",
    "raw",
    "payload",
    "data",
    "metadata",
    "properties",
    "attributes",
    "config",
    "settings",
}

DATE_FIELD_SUFFIXES = {
    "_date",
    "_dt",
    "_day",
    "_at",
    "_on",
    "_time",
    "_ts",
    "_timestamp",
}

EXPENSIVE_UNNEST_FIELD_COUNT = 5  # Arrays with more fields are expensive to UNNEST

# --- Audit Field Catalog (Consolidated) ---
# All fields related to record lifecycle, tracking, and versioning
AUDIT_FIELDS = frozenset(
    {
        # Lifecycle timestamps
        "created_at",
        "created_on",
        "creation_time",
        "create_time",
        "insert_time",
        "updated_at",
        "updated_on",
        "update_time",
        "modification_time",
        "modified_at",
        "deleted_at",
        "deleted_on",
        "deletion_time",
        "delete_time",
        "last_modified",
        "last_updated",
        "last_changed",
        # dbt standards
        "_source_created_at",
        "_source_updated_at",
        "_transform_created_at",
        "_transform_updated_at",
        # Activation/deactivation
        "activated_at",
        "deactivated_at",
        "premium_activated_at",
        "premium_deactivated_at",
        "registered_at",
        "sent_at",
        "first_active_at",
        "last_active_at",
        # Expiration
        "expiration_date",
        "default_expiration_date",
        # Actors/ownership
        "created_by",
        "creator",
        "creator_id",
        "created_by_user",
        "author",
        "updated_by",
        "updater",
        "updater_id",
        "updated_by_user",
        "modifier",
        "deleted_by",
        "deleter",
        "deleter_id",
        "deleted_by_user",
        "activated_by_id",
        "owner_id",
        "owner_name",
        # Versions
        "version",
        "revision",
        "etag",
        "row_version",
        "version_number",
        "_values_hash",
        # Soft delete state
        "is_deleted",
        "deleted",
        "active",
        "archived",
        # Source tracking
        "source",
        "source_system",
        "source_id",
        "source_table",
        "origin",
        "origin_system",
        "data_source",
        "ingested_at",
        "ingested_on",
        "ingestion_time",
        "imported_at",
        "imported_on",
        "import_time",
    }
)

# Keep legacy references for backward compatibility
AUDIT_TIMESTAMP_FIELDS = AUDIT_FIELDS
AUDIT_ACTOR_FIELDS = AUDIT_FIELDS
AUDIT_VERSION_FIELDS = AUDIT_FIELDS
AUDIT_SOFT_DELETE_FIELDS = AUDIT_FIELDS
AUDIT_SOURCE_FIELDS = AUDIT_FIELDS
ALL_AUDIT_FIELDS = AUDIT_FIELDS

# Minimum recommended audit fields for production
RECOMMENDED_AUDIT_FIELDS = {
    "created_at",  # When was this record created
    "updated_at",  # When was this record last updated
    "_transform_created_at",  # dbt standard for transformation timestamps
    "_transform_updated_at",
}

# Common audit field pairs (often used together)
AUDIT_FIELD_PAIRS = [
    ("created_at", "created_by"),  # Creation timestamp + actor
    ("updated_at", "updated_by"),  # Update timestamp + actor
    ("deleted_at", "deleted_by"),  # Deletion timestamp + actor
    ("_source_created_at", "_source_updated_at"),  # dbt source timestamp pair
    ("_transform_created_at", "_transform_updated_at"),  # dbt transform timestamp pair
    ("_valid_from", "_valid_to"),  # dbt SCD2 validity period
    ("activated_at", "activated_by_id"),  # Activation timestamp + actor
]

# --- Temporal Fields (Business Events) ---
# Business event dates and timestamps (NOT audit/system timestamps)
TEMPORAL_FIELDS = frozenset(
    {
        "calendar_date",
        "interaction_date",
        "event_date",
        "activation_date",
        "graduation_date",
        "close_date",
        "event_timestamp",
        "event_time",
    }
)

# --- Tracking Identifiers (Session/Request/Trace) ---
# Session, request, and correlation tracking IDs
TRACKING_IDENTIFIERS = frozenset(
    {
        "session_id",
        "anonymous_id",
        "session_type",
        "feed_tracking_id",
        "search_id",
        "request_id",
        "trace_id",
        "correlation_id",
    }
)

# --- System References (External Systems) ---
# References to external systems and UTM parameters
SYSTEM_REFERENCES = frozenset(
    {
        "external_id",
        "external_job_id",
        "external_job_new_id",
        "ats_id",
        "sfdc_account_id",
        "sfdc_id",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
    }
)

# Keep legacy references for backward compatibility
EVENT_TEMPORAL_FIELDS = TEMPORAL_FIELDS
SESSION_TRACKING_FIELDS = TRACKING_IDENTIFIERS
EXTERNAL_REFERENCE_FIELDS = SYSTEM_REFERENCES

# --- SCD Fields (Slowly Changing Dimensions) ---
SCD_FIELDS = frozenset(
    {
        "effective_start",
        "effective_end",
        "valid_from",
        "valid_to",
        "_valid_from",  # dbt standard
        "_valid_to",  # dbt standard
        "start_date",
        "end_date",
        "is_current",
        "is_active",
        "current_flag",
        "active_flag",
    }
)

# Keep legacy reference for backward compatibility
SCD_TYPE2_INDICATORS = SCD_FIELDS

# --- Field Naming Pattern Catalogs ---

# Boolean field prefixes
BOOLEAN_FIELD_PREFIXES = frozenset(
    {
        "is_",
        "has_",
        "can_",
        "should_",
        "will_",
        "was_",
        "were_",
    }
)

# Classification field suffixes (removed "_source" to avoid conflicts)
CLASSIFICATION_FIELD_SUFFIXES = frozenset(
    {
        "_type",
        "_category",
        "_segment",
        "_tier",
        "_level",
        "_role",
        "_channel",
        "_status",
        "_state",
        "_phase",
        "_stage",
    }
)

# Metric and aggregation field suffixes
METRIC_FIELD_SUFFIXES = frozenset(
    {
        "_count",
        "_total",
        "_sum",
        "_avg",
        "_average",
        "_mean",
        "_median",
        "_min",
        "_max",
        "_rate",
        "_ratio",
        "_percent",
        "_pct",
        "_percentage",
        "_amount",
        "_quantity",
        "_qty",
        "_rank",
        "_score",
        "_index",
        "_weight",
    }
)

# Identifier field suffixes (foreign keys + surrogate keys)
IDENTIFIER_FIELD_SUFFIXES = frozenset(
    {
        "_id",
        "_key",
        "_sk",
        "_surrogate_key",
    }
)

# NOTE: Reserved keywords, type suffixes, and other naming patterns
# are defined later in the file (see "NAMING PATTERN DETECTION" section below)

# --- Data Representation Patterns ---
NULL_REPRESENTATION_PATTERNS = {
    "null",
    "none",
    "n/a",
    "na",
    "nil",
    "undefined",
    "unknown",
    "missing",
    "empty",
}

TRUTHY_STRING_VALUES = {"true", "t", "yes", "y", "1", "on", "active", "enabled"}

FALSY_STRING_VALUES = {"false", "f", "no", "n", "0", "off", "inactive", "disabled"}

# --- Specialized Field Indicators ---
BINARY_FIELD_INDICATORS = {
    "binary",
    "bytes",
    "blob",
    "image",
    "photo",
    "file",
    "attachment",
    "document",
}

UNSTRUCTURED_ADDRESS_PATTERNS = {
    "address",
    "addr",
    "street_address",
    "mailing_address",
    "billing_address",
    "shipping_address",
    "home_address",
}

ADDRESS_COMPONENTS = {
    "street",
    "city",
    "state",
    "country",
    "zip",
    "postal",
    "province",
    "region",
}

SOFT_DELETE_FLAGS = {
    "deleted_at",
    "is_deleted",
    "deleted",
    "is_active",
    "active",
    "status",
}

# --- String Abuse Detection (fields that shouldn't be STRING) ---
STRING_FIELD_EXCLUSIONS = {
    # ISO codes and standards (legitimately STRING)
    "iso_",
    "iso",
    "_code",
    "country",
    "region",
    "state",
    "province",
}

NUMERIC_FIELD_INDICATORS = {
    "count",
    "total",
    "sum",
    "quantity",
    "amount",
    "price",
    "cost",
    "fee",
    "balance",
    "num",
    "qty",
    "score",
    "rating",
    "rank",
}

# BOOLEAN_FIELD_PREFIXES moved to line 677 with expanded set
BOOLEAN_FIELD_KEYWORDS = {"enabled", "disabled", "active"}


# =============================================================================
# DDL GENERATION CONFIGURATION
# =============================================================================

# Indentation for nested structures (2 spaces)
INDENT = "  "

# Type mapping for DDL rendering
# Note: This maps TO canonical DDL forms (FLOAT64, INT64, BOOL)
# Contrast with _canon_type() which maps FROM aliases for analysis
SCALAR_TYPE_MAP = {
    "FLOAT": "FLOAT64",
    "INTEGER": "INT64",
    "BOOLEAN": "BOOL",
}

# Retry configuration for transient errors
RETRY_MAX_ATTEMPTS = 3
RETRY_INITIAL_DELAY = 1.0  # seconds
RETRY_BACKOFF_MULTIPLIER = 2.0

# Parallelization configuration
PARALLEL_WORKERS = 15  # Number of threads for parallel table processing
PARALLEL_ENABLED_DEFAULT = True  # Enable parallel processing by default


# =============================================================================
# ANTI-PATTERN DETECTION: TEMPORAL & TIME-BASED PATTERNS
# =============================================================================

# Epoch/Unix timestamp field indicators (Anti-pattern #32)
EPOCH_TIME_INDICATORS = frozenset(
    {
        "_epoch",
        "_ts",
        "_ms",
        "_seconds",
        "epoch",
        "unix",
    }
)

# Duration field suffixes (Anti-pattern #33)
DURATION_FIELD_SUFFIXES = frozenset(
    {
        "_duration",
        "_ttl",
        "_age",
        "_latency",
        "_elapsed",
    }
)

# Event timestamp field indicators (for partitioning detection)
EVENT_TIME_INDICATORS = frozenset(
    {
        "event_time",
        "event_timestamp",
        "created_at",
        "occurred_at",
        "timestamp",
    }
)

# Instant event indicators (for DATETIME vs TIMESTAMP check, Anti-pattern #36)
INSTANT_EVENT_INDICATORS = frozenset(
    {
        "_at",
        "_timestamp",
        "created",
        "updated",
        "occurred",
        "published",
        "sent",
        "received",
    }
)


# =============================================================================
# ANTI-PATTERN DETECTION: MONETARY & FINANCIAL PATTERNS
# =============================================================================

# Monetary field indicators (Anti-pattern #34)
MONEY_FIELD_INDICATORS = frozenset(
    {
        "amount",
        "price",
        "cost",
        "fee",
        "revenue",
        "balance",
        "payment",
        "total",
    }
)


# =============================================================================
# ANTI-PATTERN DETECTION: GEOSPATIAL PATTERNS
# =============================================================================

# Latitude field names (Anti-pattern #35)
LATITUDE_FIELD_NAMES = frozenset({"lat", "latitude"})

# Longitude field names (Anti-pattern #35)
LONGITUDE_FIELD_NAMES = frozenset({"lon", "lng", "longitude"})


# =============================================================================
# ANTI-PATTERN DETECTION: ARRAY & NESTED STRUCTURE PATTERNS
# =============================================================================

# Natural key field names (for array identification, Anti-pattern #40)
ARRAY_NATURAL_KEY_NAMES = frozenset(
    {
        "id",
        "uuid",
        "guid",
        "key",
        "index",
        "code",
        "name",
        "type",
        "identifier",
    }
)

# Minimum field count for "god child" array detection (Anti-pattern #41)
GOD_ARRAY_MIN_FIELDS = 20
GOD_ARRAY_MIN_DEPTH = 3


# =============================================================================
# ANTI-PATTERN DETECTION: CLUSTERING & PARTITIONING
# =============================================================================

# Low-cardinality field indicators (for clustering analysis, Anti-pattern #44)
LOW_CARDINALITY_INDICATORS = frozenset(
    {
        "status",
        "state",
        "type",
        "flag",
        "is_",
    }
)


# =============================================================================
# ANTI-PATTERN DETECTION: DATASET-LEVEL PATTERNS
# =============================================================================

# Minimum shard count to flag sharded table pattern (Anti-pattern #46)
MIN_SHARD_COUNT = 3


# =============================================================================
# SCHEMA ANTI-PATTERN DETECTION: NAMING & CONVENTIONS
# =============================================================================

# Generic field names that should be avoided (Anti-pattern detection)
GENERIC_FIELD_NAMES = frozenset(
    {
        "data",
        "value",
        "info",
        "details",
        "metadata",
        "content",
    }
)

# Partition field name indicators (for nullable partition field detection)
PARTITION_FIELD_NAMES = frozenset(
    {
        "partition_date",
        "event_date",
        "date",
        "_partitiontime",
    }
)

# Parent/hierarchy field indicators
PARENT_FIELD_INDICATORS = frozenset(
    {
        "parent_id",
        "parent_key",
        "manager_id",
        "superior_id",
        "parent",
    }
)

# Hierarchy helper field names
HIERARCHY_HELPER_FIELDS = frozenset(
    {
        "level",
        "depth",
        "path",
        "lineage",
        "is_leaf",
        "leaf_flag",
    }
)

# Milestone date indicators for accumulating snapshot fact tables
MILESTONE_DATE_INDICATORS = frozenset(
    {
        "order",
        "ship",
        "deliver",
        "start",
        "end",
        "complete",
        "cancel",
        "approve",
    }
)


# =============================================================================
# DEGENERATE DIMENSION DETECTION
# =============================================================================

# Degenerate dimension suffixes (high-cardinality codes in fact tables)
DEGENERATE_DIMENSION_SUFFIXES = frozenset(
    {
        "_number",
        "_code",
        "_id",
        "_reference",
        "_ref",
        "_ticket",
        "_confirmation",
        "_tracking",
    }
)

# Degenerate dimension prefixes
DEGENERATE_DIMENSION_PREFIXES = frozenset(
    {
        "order_",
        "invoice_",
        "po_",
        "ticket_",
        "transaction_",
        "confirmation_",
        "tracking_",
        "reference_",
    }
)

# Common degenerate dimension patterns (exact matches)
DEGENERATE_DIMENSION_PATTERNS = frozenset(
    {
        "order_number",
        "invoice_number",
        "po_number",
        "transaction_id",
        "confirmation_number",
        "tracking_number",
        "ticket_id",
        "case_number",
        "claim_number",
        "serial_number",
        "batch_number",
        "lot_number",
    }
)


# =============================================================================
# DIMENSIONAL MODELING - ADVANCED DETECTION
# =============================================================================

# Bridge table detection thresholds
BRIDGE_TABLE_MAX_FK_COUNT = 3  # Max FKs for bridge table
BRIDGE_TABLE_MIN_FK_COUNT = 2  # Min FKs for bridge table
BRIDGE_TABLE_MAX_MEASURES = 1  # Max numeric non-FK columns
BRIDGE_TABLE_MAX_ATTRIBUTES = 3  # Max non-key attributes

# Natural key indicators for dimensions
DIMENSION_NATURAL_KEY_SUFFIXES = frozenset(
    {
        "_code",
        "_number",
        "_name",
        "_identifier",
        "_ref",
    }
)

# Surrogate key indicators
DIMENSION_SURROGATE_KEY_NAMES = frozenset(
    {
        "id",
        "key",
        "sk",
        "surrogate_key",
    }
)

DIMENSION_SURROGATE_KEY_SUFFIXES = frozenset(
    {
        "_key",
        "_sk",
        "_id",
    }
)

# Snowflake depth limit
SNOWFLAKE_MAX_DEPTH = 1  # Warn if chains deeper than this

# Fact table FK density thresholds
FACT_MIN_FK_COUNT = 2  # Too denormalized if fewer
FACT_MAX_FK_COUNT = 12  # God fact if more

# Snapshot fact indicators
SNAPSHOT_FACT_DATE_FIELDS = frozenset(
    {
        "as_of_date",
        "snapshot_date",
        "valid_on",
        "report_date",
        "effective_date",
    }
)

# Rapidly changing attribute keywords (mini-dimension candidates)
RAPIDLY_CHANGING_ATTRIBUTE_KEYWORDS = frozenset(
    {
        "_preference",
        "_status",
        "_setting",
        "_flag",
        "_option",
        "_choice",
        "_state",
    }
)

# Unit of measure indicators
UOM_MEASURE_KEYWORDS = frozenset(
    {
        "length",
        "width",
        "height",
        "depth",
        "weight",
        "amount",
        "distance",
        "volume",
        "mass",
        "temperature",
        "duration",
        "speed",
        "velocity",
    }
)

UOM_COLUMN_SUFFIXES = frozenset(
    {
        "_uom",
        "_unit",
        "_units",
        "_measurement_unit",
    }
)

# Code/value pair indicators
CODE_VALUE_PAIRS = frozenset(
    {
        ("status_code", "status_text"),
        ("status_code", "status_name"),
        ("status_code", "status_description"),
        ("type_code", "type_text"),
        ("type_code", "type_name"),
        ("type_code", "type_description"),
        ("category_code", "category_text"),
        ("category_code", "category_name"),
        ("state_code", "state_text"),
        ("state_code", "state_name"),
    }
)

# Unknown/N/A member indicators
UNKNOWN_MEMBER_VALUES = frozenset(
    {
        "unknown",
        "n/a",
        "na",
        "not_applicable",
        "none",
        "unspecified",
        "missing",
        "pending",
    }
)

# Mixed-grain identifiers (header + line)
HEADER_ID_KEYWORDS = frozenset(
    {
        "order_id",
        "invoice_id",
        "document_id",
        "header_id",
        "parent_id",
        "request_id",
    }
)

LINE_ITEM_ID_KEYWORDS = frozenset(
    {
        "line_item_id",
        "line_id",
        "item_id",
        "detail_id",
        "position",
        "line_number",
        "sequence_number",
    }
)

# Date dimension completeness indicators
DATE_DIM_FISCAL_FIELDS = frozenset(
    {
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_month",
        "fiscal_week",
        "fiscal_period",
    }
)

DATE_DIM_WEEK_FIELDS = frozenset(
    {
        "week_of_year",
        "iso_week",
        "week_number",
        "week_start_date",
        "week_end_date",
    }
)


# =============================================================================
# SQL RESERVED KEYWORDS (BigQuery)
# =============================================================================

# BigQuery SQL reserved keywords that should be avoided as field names
RESERVED_KEYWORDS = frozenset(
    {
        "all",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "assert_rows_modified",
        "at",
        "between",
        "by",
        "case",
        "cast",
        "collate",
        "contains",
        "create",
        "cross",
        "cube",
        "current",
        "default",
        "define",
        "desc",
        "distinct",
        "else",
        "end",
        "enum",
        "escape",
        "except",
        "exclude",
        "exists",
        "extract",
        "false",
        "fetch",
        "following",
        "for",
        "from",
        "full",
        "group",
        "grouping",
        "groups",
        "hash",
        "having",
        "if",
        "ignore",
        "in",
        "inner",
        "intersect",
        "interval",
        "into",
        "is",
        "join",
        "lateral",
        "left",
        "like",
        "limit",
        "lookup",
        "merge",
        "natural",
        "new",
        "no",
        "not",
        "null",
        "nulls",
        "of",
        "on",
        "or",
        "order",
        "outer",
        "over",
        "partition",
        "preceding",
        "proto",
        "range",
        "recursive",
        "respect",
        "right",
        "rollup",
        "rows",
        "select",
        "set",
        "some",
        "struct",
        "tablesample",
        "then",
        "to",
        "treat",
        "true",
        "unbounded",
        "union",
        "unnest",
        "using",
        "when",
        "where",
        "window",
        "with",
        "within",
    }
)


# =============================================================================
# DATE/TIME TYPE DETECTION
# =============================================================================

# Fields that should use DATE instead of TIMESTAMP (date-only semantics)
DATE_ONLY_FIELD_PATTERNS = frozenset(
    {
        "birth_date",
        "birthdate",
        "date_of_birth",
        "dob",
        "hire_date",
        "hired_date",
        "start_date",
        "end_date",
        "expiry_date",
        "expiration_date",
        "due_date",
        "publish_date",
        "published_date",
        "release_date",
    }
)


# =============================================================================
# BINARY DATA DETECTION
# =============================================================================

# Keywords indicating binary data (should use BYTES, not STRING)
BINARY_DATA_KEYWORDS = frozenset(
    {
        "hash",
        "digest",
        "signature",
        "checksum",
        "image_data",
        "file_content",
        "binary",
        "hex",
        "base64",
    }
)


# =============================================================================
# NAMING PATTERN DETECTION
# =============================================================================

# Negative boolean field name patterns (anti-pattern #22)
NEGATIVE_BOOLEAN_PATTERNS = frozenset(
    {
        "is_not_",
        "has_no_",
        "cannot_",
        "isnt_",
        "hasnt_",
        "no_",
        "not_",
        "non_",
        "without_",
        "disabled",
        "inactive",
    }
)

# Type suffix patterns that should be avoided in field names (anti-pattern #35)
TYPE_SUFFIX_PATTERNS = frozenset(
    {
        "_string",
        "_str",
        "_int",
        "_integer",
        "_bool",
        "_boolean",
        "_array",
        "_list",
        "_dict",
        "_map",
    }
)

# Enum-like field indicators (anti-pattern #33)
ENUM_FIELD_INDICATORS = frozenset(
    {
        "status",
        "state",
        "type",
        "category",
        "level",
        "priority",
        "role",
        "kind",
    }
)

# Float/money field keywords (anti-pattern #27)
FLOAT_MONEY_KEYWORDS = frozenset(
    {
        "price",
        "cost",
        "amount",
        "balance",
        "tax",
        "fee",
        "payment",
        "salary",
        "revenue",
        "total",
    }
)
