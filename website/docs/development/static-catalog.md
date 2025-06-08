---
id: static-catalog
title: Static Catalog
---

# Static Catalog

The Static Catalog feature allows Wvlet to compile queries without making remote catalog calls, significantly improving compilation performance. This is especially useful for CI/CD pipelines, offline development, and scenarios where catalog metadata doesn't change frequently.

## Overview

By default, Wvlet's compiler fetches catalog metadata (schemas, tables, columns) from remote databases during compilation. This can cause performance issues when:
- Multiple table references require repeated remote calls
- Network latency is high
- Catalog cache expires (default: 5 minutes)

The Static Catalog feature addresses these issues by loading catalog metadata from local JSON files.

## Architecture

### File Structure

Static catalogs are organized by database type and catalog name:

```
<basePath>/
├── duckdb/
│   └── my_catalog/
│       ├── schemas.json        # List of database schemas
│       ├── main.json          # Tables in 'main' schema
│       ├── analytics.json     # Tables in 'analytics' schema
│       └── functions.json     # SQL functions
└── trino/
    └── another_catalog/
        └── ...
```

### JSON Format

#### schemas.json
```json
[
  {
    "catalog": "my_catalog",
    "name": "main",
    "description": "Main schema",
    "properties": {}
  }
]
```

#### `<schema_name>.json` (e.g., main.json)
```json
[
  {
    "tableName": {
      "catalog": "my_catalog",
      "schema": "main",
      "name": "users"
    },
    "columns": [
      {"name": "id", "dataType": {"typeName": "long"}},
      {"name": "name", "dataType": {"typeName": "string"}},
      {"name": "email", "dataType": {"typeName": "string"}}
    ],
    "description": "User table",
    "properties": {}
  }
]
```

#### functions.json
```json
[
  {
    "name": "sum",
    "functionType": "AGGREGATE",
    "args": [{"typeName": "double"}],
    "returnType": {"typeName": "double"}
  }
]
```

## Usage

### CLI Usage

The recommended way to use static catalogs is through the Wvlet CLI:

```bash
# Import catalog from database
wv catalog import --name mydb

# List available catalogs
wv catalog list

# Show catalog details
wv catalog show duckdb/mydb

# Compile with static catalog
wvlet compile -f query.wv --use-static-catalog --catalog mydb
```

See [Catalog Management](../usage/catalog-management.md) for detailed CLI usage.

### Programmatic Usage

```scala
import wvlet.lang.compiler.{Compiler, CompilerOptions, DBType, WorkEnv}
import wvlet.log.LogLevel

val workEnv = WorkEnv(".", LogLevel.INFO)

val compilerOptions = CompilerOptions(
  sourceFolders = List("."),
  workEnv = workEnv,
  catalog = Some("my_catalog"),
  schema = Some("main"),
  dbType = DBType.DuckDB,
  useStaticCatalog = true,
  staticCatalogPath = Some("/path/to/catalog/base")
)

val compiler = Compiler(compilerOptions)
val result = compiler.compile()
```

### Configuration Options

- `sourceFolders`: List of directories containing .wv files
- `workEnv`: Working environment with path and log level
- `catalog`: Catalog name to load
- `schema`: Default schema name
- `dbType`: Target database type (DuckDB, Trino, etc.)
- `useStaticCatalog`: Boolean flag to enable static catalog mode
- `staticCatalogPath`: Base directory containing catalog metadata

## Implementation Details

### Key Components

1. **StaticCatalog**: Implements the `Catalog` trait with read-only operations
2. **StaticCatalogProvider**: Loads catalogs from filesystem with error handling
3. **CatalogSerializer**: Handles JSON serialization/deserialization
4. **CompilerOptions**: Extended with static catalog configuration

### Error Handling

- Missing files: Falls back to empty collections (schemas, tables, functions)
- Corrupted JSON: Falls back to InMemoryCatalog
- Write operations: Throw `NOT_IMPLEMENTED` exceptions
- Missing resources: Throw appropriate `NOT_FOUND` exceptions

### Performance Characteristics

- **Initial load**: One-time filesystem read at compiler initialization
- **Lookups**: O(1) HashMap lookups for schemas and tables
- **Memory usage**: Proportional to catalog size (all metadata loaded in memory)

## Platform Support

- **JVM**: Full support for static catalog loading from filesystem
- **Scala.js**: Not supported (no file I/O capabilities)
- **Scala Native**: Not supported (limited file I/O support)

## Limitations

- Read-only: Cannot create or modify schemas/tables
- Manual updates: Catalog files must be updated externally
- No automatic refresh: Changes require compiler restart
- Platform-specific: Only available on JVM platform

## Future Enhancements

- **Incremental Updates**: Support for updating only changed schemas/tables
- **Schema Evolution**: Handle schema changes gracefully with versioning
- **Partial Loading**: Load only required schemas for better performance
- **Remote Storage**: Support for S3, GCS, and other cloud storage backends
- **Catalog Diff**: Show differences between catalog versions
- **Multi-Platform Support**: Extend to Scala.js and Scala Native platforms
