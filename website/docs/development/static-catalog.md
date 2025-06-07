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

### Programmatic Usage

```scala
import wvlet.lang.compiler.{Compiler, CompilerOptions, DBType}
import wvlet.log.LogLevel

val compilerOptions = CompilerOptions(
  workEnv = WorkEnv(".", logLevel = LogLevel.INFO),
  catalog = Some("my_catalog"),
  schema = Some("main"),
  dbType = DBType.DuckDB
).withStaticCatalog("/path/to/catalog/base")

val compiler = Compiler(compilerOptions)
val result = compiler.compile()
```

### Configuration Options

- `staticCatalogPath`: Base directory containing catalog metadata
- `useStaticCatalog`: Boolean flag to enable static catalog mode
- `dbType`: Database type (DuckDB, Trino, etc.)
- `catalog`: Catalog name to load
- `schema`: Default schema name

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

- **Phase 2**: CLI commands for importing/exporting catalog metadata
- **Phase 3**: Automatic catalog refresh and incremental updates
- **Phase 4**: Support for partial catalogs and lazy loading
