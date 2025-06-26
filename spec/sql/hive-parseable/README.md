# Hive SQL Parse Tests

This directory contains test files for Hive SQL parsing and generation that are compatible with the standard SQL parser.

## Files

- `wvlet-to-hive.wv` - Wvlet queries that demonstrate Hive SQL generation features (array syntax, struct syntax)
- `basic-queries.sql` - Standard SQL queries that can be parsed and generated as Hive SQL
- `values-syntax.sql` - Test INSERT statements to verify Hive VALUES clause formatting

## Testing

These files are tested by:
- `HiveParseSpec` - Custom test that focuses on parsing and SQL generation without execution
  - Located in `wvlet-runner/src/test/scala/wvlet/lang/runner/HiveParseSpec.scala`
  - Tests both Wvlet queries and SQL queries
  - Verifies Hive-specific SQL syntax generation

## Note on Function Transformations

Function transformations (e.g., `array_agg` → `collect_list`) require the full compilation pipeline with transformations enabled. The parse-only tests verify SQL syntax generation but do not apply these transformations.