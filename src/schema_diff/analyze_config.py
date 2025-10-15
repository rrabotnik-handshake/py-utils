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

# --- Audit Field Catalogs ---
AUDIT_TIMESTAMP_FIELDS = {
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
}

AUDIT_ACTOR_FIELDS = {
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
}

AUDIT_VERSION_FIELDS = {
    "version",
    "revision",
    "etag",
    "row_version",
    "version_number",
}

AUDIT_SOFT_DELETE_FIELDS = {
    "deleted_at",
    "is_deleted",
    "deleted",
    "is_active",
    "active",
    "archived",
}

AUDIT_SOURCE_FIELDS = {
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

# All audit fields combined
ALL_AUDIT_FIELDS = (
    AUDIT_TIMESTAMP_FIELDS
    | AUDIT_ACTOR_FIELDS
    | AUDIT_VERSION_FIELDS
    | AUDIT_SOFT_DELETE_FIELDS
    | AUDIT_SOURCE_FIELDS
)

# Minimum recommended audit fields for production
RECOMMENDED_AUDIT_FIELDS = {
    "created_at",  # When was this record created
    "updated_at",  # When was this record last updated
}

# Common audit field pairs (often used together)
AUDIT_FIELD_PAIRS = [
    ("created_at", "created_by"),  # Creation timestamp + actor
    ("updated_at", "updated_by"),  # Update timestamp + actor
    ("deleted_at", "deleted_by"),  # Deletion timestamp + actor
]

# --- Reserved Keywords ---
RESERVED_KEYWORDS = {
    "select",
    "from",
    "where",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "group",
    "order",
    "by",
    "having",
    "limit",
    "offset",
    "union",
    "all",
    "distinct",
    "case",
    "when",
    "then",
    "else",
    "end",
    "as",
    "and",
    "or",
    "not",
    "in",
    "exists",
    "between",
    "like",
    "is",
    "null",
    "true",
    "false",
    "cast",
    "convert",
    "table",
    "view",
    "index",
    "key",
    "primary",
    "foreign",
    "references",
    "constraint",
    "create",
    "alter",
    "drop",
    "insert",
    "update",
    "delete",
    "truncate",
    "grant",
    "revoke",
    "commit",
    "rollback",
    "transaction",
}

# --- Type-Related Patterns ---
TYPE_SUFFIXES = {
    "_string",
    "_str",
    "_int",
    "_integer",
    "_float",
    "_bool",
    "_boolean",
    "_array",
    "_list",
}

NEGATIVE_BOOLEAN_PATTERNS = {"is_not_", "not_", "isnt_", "disabled", "inactive"}

ENUM_FIELD_INDICATORS = {
    "status",
    "state",
    "type",
    "kind",
    "category",
    "role",
    "level",
    "priority",
}

DATE_ONLY_FIELD_PATTERNS = {
    "birth_date",
    "birthdate",
    "hire_date",
    "start_date",
    "end_date",
    "expiry_date",
    "expiration_date",
    "effective_date",
    "due_date",
}

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

BOOLEAN_FIELD_PREFIXES = {"is_", "has_", "can_", "should_", "will_"}
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
