# Hive SQL Reference Examples

This directory contains reference examples of Hive SQL syntax as generated by Wvlet's Hive SQL generator.

## Important Note

These SQL files demonstrate Hive-specific syntax and are **not meant to be executed directly** by the test runner. They serve as:

1. **Reference documentation** for Hive SQL features supported by Wvlet
2. **Expected output examples** for the Hive SQL generation tests
3. **Learning resources** for understanding Hive SQL dialect differences

## Files

- `hive-functions.sql` - Hive-specific function mappings (e.g., collect_list, regexp)
- `hive-data-types.sql` - Hive data type syntax (arrays, maps, structs)
- `hive-lateral-view.sql` - LATERAL VIEW examples (Hive's alternative to UNNEST)

## Testing

The actual testing of Hive SQL generation is done in:
- `wvlet-lang/src/test/scala/wvlet/lang/compiler/codegen/HiveSqlGeneratorTest.scala`

These tests verify that Wvlet correctly generates Hive-compatible SQL from Wvlet queries.