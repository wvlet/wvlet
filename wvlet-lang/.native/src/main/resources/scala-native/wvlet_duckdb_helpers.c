/*
 * Tiny C shims for DuckDB functions that take/return structs by value.
 *
 * Scala Native's @extern ABI for CStruct parameters doesn't currently match the System V
 * calling convention for structs larger than 16 bytes (`duckdb_result` is 48 bytes), so
 * `duckdb_fetch_chunk(*resultPtr)` from a Scala Native binding returns null on the first
 * fetch — the C function never sees the right bytes. These wrappers take a `duckdb_result *`
 * (which Scala Native passes correctly) and forward by value to the real DuckDB C API.
 *
 * Compiled and linked automatically — Scala Native picks up any .c / .cpp in
 * src/main/resources/scala-native/ and links it into the consumer binary.
 *
 * See plans/2026-05-13-duckdb-execute-followups.md for context.
 */

#include "duckdb.h"

duckdb_data_chunk wvlet_duckdb_fetch_chunk(duckdb_result *result) {
  return duckdb_fetch_chunk(*result);
}

idx_t wvlet_duckdb_result_chunk_count(duckdb_result *result) {
  return duckdb_result_chunk_count(*result);
}

uint32_t wvlet_duckdb_string_t_length(duckdb_string_t *string) {
  return duckdb_string_t_length(*string);
}
