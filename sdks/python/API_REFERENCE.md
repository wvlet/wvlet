# Wvlet Python SDK API Reference

This document provides a complete API reference for the Wvlet Python SDK.

## Module: `wvlet`

The main module providing the primary interface for Wvlet compilation.

### Functions

#### `compile(query: str, target: Optional[str] = None) -> str`

Compile a Wvlet query to SQL.

**Parameters:**
- `query` (str): The Wvlet query string to compile
- `target` (Optional[str]): Target SQL dialect. Valid values:
  - `"duckdb"` - DuckDB SQL dialect
  - `"trino"` - Trino/Presto SQL dialect
  - `None` - Use default SQL dialect

**Returns:**
- str: The compiled SQL query string

**Raises:**
- `CompilationError`: If the query has syntax errors or cannot be compiled
- `NotImplementedError`: If the native library is not available for the current platform

**Example:**
```python
from wvlet import compile

sql = compile("from users select name, age where age > 18")
print(sql)  # SELECT name, age FROM users WHERE age > 18

# With specific target
sql = compile("from logs select count(*)", target="trino")
```

## Module: `wvlet.compiler`

Contains the main compiler implementation and related utilities.

### Classes

#### `WvletCompiler`

The main compiler class that handles Wvlet query compilation.

##### Constructor

```python
WvletCompiler(target: Optional[str] = None, wvlet_home: Optional[str] = None)
```

**Parameters:**
- `target` (Optional[str]): Default target SQL dialect for this compiler instance
- `wvlet_home` (Optional[str]): Path to Wvlet home directory (defaults to `~/.wvlet`). Reserved for future functionality such as caching, user catalogs, and local schema management. Currently not used.

**Example:**
```python
from wvlet.compiler import WvletCompiler

# Create compiler with default settings
compiler = WvletCompiler()

# Create compiler for specific target
trino_compiler = WvletCompiler(target="trino")

# Create compiler with custom home directory
custom_compiler = WvletCompiler(wvlet_home="/opt/wvlet")
```

##### Methods

###### `compile(query: str, target: Optional[str] = None) -> str`

Compile a Wvlet query to SQL.

**Parameters:**
- `query` (str): The Wvlet query to compile
- `target` (Optional[str]): Override the default target for this compilation

**Returns:**
- str: The compiled SQL query

**Raises:**
- `CompilationError`: If compilation fails
- `NotImplementedError`: If the native library is not available for the current platform

**Example:**
```python
compiler = WvletCompiler(target="duckdb")

# Use default target (duckdb)
sql1 = compiler.compile("from users select *")

# Override target for specific query
sql2 = compiler.compile("from logs select *", target="trino")
```

### Exceptions

#### `CompilationError`

Raised when a Wvlet query cannot be compiled due to syntax or semantic errors.

**Attributes:**
- `message` (str): The error message
- `line` (Optional[int]): Line number where the error occurred (if available)
- `column` (Optional[int]): Column number where the error occurred (if available)

**Example:**
```python
from wvlet import compile
from wvlet.compiler import CompilationError

try:
    sql = compile("invalid syntax here")
except CompilationError as e:
    print(f"Error: {e.message}")
    if e.line:
        print(f"Line: {e.line}")
```

## Environment Variables

The SDK behavior can be controlled through environment variables:

### `WVLET_HOME`

Path to the Wvlet home directory. Defaults to `~/.wvlet`.

```bash
export WVLET_HOME=/opt/wvlet
```


### `WVLET_LOG_LEVEL`

Set logging level for debugging.

```bash
export WVLET_LOG_LEVEL=DEBUG
```

## Native Library Loading

The SDK attempts to load the native library in the following order:

1. From the bundled `wvlet/libs/` directory based on platform:
   - Linux x86_64: `wvlet/libs/linux_x86_64/libwvlet.so`
   - Linux ARM64: `wvlet/libs/linux_aarch64/libwvlet.so`
   - macOS ARM64: `wvlet/libs/darwin_arm64/libwvlet.dylib`

2. From system library paths (if installed separately)

## Query Syntax

The SDK supports the full Wvlet query syntax. Here are the main constructs:

### Basic Queries

```python
# SELECT
compile("from table select col1, col2")

# WHERE
compile("from table where condition select *")

# ORDER BY
compile("from table select * order by col1 desc")

# LIMIT
compile("from table select * limit 10")
```

### Aggregations

```python
# GROUP BY
compile("""
from sales
group by category
agg sum(amount) as total
""")

# HAVING
compile("""
from sales
group by category
agg sum(amount) as total
having sum(amount) > 1000
""")
```

### Joins

```python
# INNER JOIN
compile("""
from orders o
join customers c on o.customer_id = c.id
select *
""")

# LEFT JOIN
compile("""
from orders o
left join customers c on o.customer_id = c.id
select *
""")
```

### Models (CTEs)

```python
compile("""
model TempResults = {
    from source_table
    where condition
}

from TempResults
select count(*)
""")
```

### Window Functions

```python
compile("""
from sales
select 
    *,
    row_number() over (partition by category order by amount desc) as rank
""")
```

## Type System

Wvlet supports the following SQL data types:

- **Numeric**: `int`, `long`, `decimal`, `float`, `double`
- **String**: `string`, `varchar(n)`, `char(n)`
- **Temporal**: `date`, `time`, `timestamp`
- **Boolean**: `boolean`
- **Complex**: `array<T>`, `map<K,V>`, `struct<...>`
- **Special**: `null`, `any`

## Best Practices

1. **Query Formatting**: Use multi-line strings for complex queries:
   ```python
   query = """
   from users
   where active = true
   select user_id, email
   order by created_at desc
   """
   sql = compile(query)
   ```

2. **Error Handling**: Always handle compilation errors:
   ```python
   try:
       sql = compile(query)
   except CompilationError as e:
       logger.error(f"Query compilation failed: {e}")
       raise
   ```

3. **Target Selection**: Choose appropriate target for your database:
   ```python
   # For DuckDB
   sql = compile(query, target="duckdb")
   
   # For Trino/Presto
   sql = compile(query, target="trino")
   ```

4. **Performance**: Reuse compiler instances for multiple queries:
   ```python
   compiler = WvletCompiler(target="duckdb")
   for query in queries:
       sql = compiler.compile(query)
   ```

## Limitations

- Native library currently supports Linux (x86_64, ARM64) and macOS (ARM64)
- Windows support is planned for future releases
- Some advanced Wvlet features may not be available in all SQL targets

## Troubleshooting

### Common Issues

1. **Native library not found**
   - Ensure you have the latest version: `pip install --upgrade wvlet`
   - Check platform compatibility
   - Verify your platform is supported

2. **Compilation errors**
   - Verify query syntax
   - Check for typos in table/column names
   - Ensure proper quote escaping in strings

3. **Performance issues**
   - Ensure native library is properly loaded
   - Reuse compiler instances
   - Consider batch compilation for multiple queries

### Debug Mode

Enable debug logging to troubleshoot issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from wvlet import compile
sql = compile("from users select *")  # Will show debug output
```

## Version Information

```python
import wvlet
print(wvlet.__version__)  # Print SDK version
```

## Links

- [GitHub Repository](https://github.com/wvlet/wvlet)
- [PyPI Package](https://pypi.org/project/wvlet/)
- [Issue Tracker](https://github.com/wvlet/wvlet/issues)
- [Wvlet Documentation](https://wvlet.org/docs/)
