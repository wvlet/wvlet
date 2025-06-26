# Wvlet Python SDK

Python SDK for [Wvlet](https://wvlet.org) - A flow-style query language for functional data modeling and interactive data exploration.

## Features

- 🚀 **Fast native compilation** - Uses bundled native library for high-performance query compilation
- 🎯 **Multiple SQL targets** - Supports DuckDB, Trino, and other SQL engines
- 📦 **Zero dependencies** - Pure Python with native acceleration
- 🐍 **Pythonic API** - Simple and intuitive interface

## Installation

### From PyPI

```bash
pip install wvlet
```

### From Source

```bash
# Install latest development version
pip install git+https://github.com/wvlet/wvlet.git#subdirectory=sdks/python

# Install editable version for development
git clone https://github.com/wvlet/wvlet.git
cd wvlet/sdks/python
pip install -e .
```

## Quick Start

```python
from wvlet import compile

# Compile a simple query
sql = compile("from users select name, age where age > 18")
print(sql)
# Output: SELECT name, age FROM users WHERE age > 18

# Use model references
sql = compile("""
model UserStats = {
  from users
  group by user_id
  agg count(*) as event_count
}

from UserStats
where event_count > 100
""")
```

## Usage Guide

### Basic Compilation

```python
from wvlet import compile

# Simple select
sql = compile("from products select name, price")

# With filtering
sql = compile("from orders where status = 'completed' select order_id, total")

# With joins
sql = compile("""
from orders o
join customers c on o.customer_id = c.id
select o.order_id, c.name, o.total
""")
```

### Using the Compiler Class

```python
from wvlet.compiler import WvletCompiler

# Create compiler with specific target
compiler = WvletCompiler(target="trino")

# Compile multiple queries
queries = [
    "from users select count(*)",
    "from products where price > 100 select name, price"
]

for query in queries:
    sql = compiler.compile(query)
    print(sql)
```

### Advanced Features

#### Window Functions

```python
sql = compile("""
from sales
select 
  date,
  amount,
  sum(amount) over (order by date rows 7 preceding) as rolling_7day_sum
""")
```

#### CTEs and Models

```python
sql = compile("""
model ActiveUsers = {
  from users 
  where last_login > current_date - interval '30' day
  select user_id, email
}

from ActiveUsers
select count(*) as active_user_count
""")
```

#### Pivoting Data

```python
sql = compile("""
from sales
group by date
pivot sum(amount) for category in ('Electronics', 'Clothing', 'Food')
""")
```

### Error Handling

```python
from wvlet import compile
from wvlet.compiler import CompilationError

try:
    sql = compile("invalid query syntax")
except CompilationError as e:
    print(f"Compilation failed: {e}")
    # Access detailed error information
    if hasattr(e, 'line'):
        print(f"Error at line {e.line}: {e.message}")
```

## API Reference

### `wvlet.compile(query: str, target: str = None) -> str`

Compile a Wvlet query to SQL.

**Parameters:**
- `query` (str): The Wvlet query to compile
- `target` (str, optional): Target SQL dialect ("duckdb", "trino", etc.)

**Returns:**
- str: The compiled SQL query

**Raises:**
- `CompilationError`: If the query cannot be compiled
- `NotImplementedError`: If the native library is not available for the current platform

### `wvlet.compiler.WvletCompiler`

Main compiler class for more control over compilation.

#### `__init__(self, target: str = None, wvlet_home: str = None)`

Initialize a new compiler instance.

**Parameters:**
- `target` (str, optional): Default target SQL dialect
- `wvlet_home` (str, optional): Path to Wvlet home directory

#### `compile(self, query: str, target: str = None) -> str`

Compile a Wvlet query to SQL.

**Parameters:**
- `query` (str): The Wvlet query to compile
- `target` (str, optional): Override default target for this compilation

**Returns:**
- str: The compiled SQL query

## Supported Platforms

### Native Library Support

Platform | Architecture | Status
---------|-------------|--------
Linux    | x86_64      | ✅ Supported
Linux    | aarch64     | ✅ Supported
macOS    | arm64       | ✅ Supported
Windows  | x86_64      | 🔄 Planned


## Performance

The native library provides high-performance query compilation:

```python
import time
from wvlet import compile

# Benchmark native compilation
start = time.time()
for _ in range(100):
    compile("from users select * where age > 21")
print(f"Compiled 100 queries in {time.time() - start:.2f}s")
```

## Integration Examples

### With Pandas and DuckDB

```python
import pandas as pd
import duckdb
from wvlet import compile

# Compile Wvlet to SQL
wvlet_query = """
from sales.csv
where region = 'North America'
group by date
agg sum(amount) as total_sales
order by date
"""

sql = compile(wvlet_query, target="duckdb")

# Execute with DuckDB
conn = duckdb.connect()
df = conn.execute(sql).fetchdf()
```

### With SQLAlchemy

```python
from sqlalchemy import create_engine, text
from wvlet import compile

engine = create_engine("postgresql://user:pass@localhost/db")

wvlet_query = """
from orders o
join customers c on o.customer_id = c.id
where o.created_at > current_date - 7
group by c.name
agg sum(o.total) as weekly_total
"""

sql = compile(wvlet_query)  # Uses default SQL dialect

with engine.connect() as conn:
    result = conn.execute(text(sql))
    for row in result:
        print(row)
```

## Development

### Setting up Development Environment

```bash
# Clone the repository
git clone https://github.com/wvlet/wvlet.git
cd wvlet/sdks/python

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=wvlet

# Run specific test
pytest tests/test_compiler.py::test_basic_compilation
```

### Building Wheels

```bash
# Build source distribution and wheel
python -m build

# Build platform-specific wheel
python -m build --wheel
```

## Troubleshooting

### Native Library Not Found

If you see an error about the native library not being found:

1. Check your platform is supported (see Supported Platforms)
2. Ensure you have the latest version: `pip install --upgrade wvlet`
3. Try reinstalling: `pip install --force-reinstall wvlet`
4. Use CLI fallback by installing the Wvlet CLI

### Compilation Errors

For query compilation errors:

1. Check query syntax matches [Wvlet documentation](https://wvlet.org/docs/syntax/)
2. Use the CLI to validate: `wvlet compile "your query"`
3. Enable debug logging:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

## Contributing

Contributions are welcome! Please see the [main project contributing guide](https://github.com/wvlet/wvlet/blob/main/CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](https://github.com/wvlet/wvlet/blob/main/LICENSE) for details.

## Links

- [Wvlet Documentation](https://wvlet.org)
- [GitHub Repository](https://github.com/wvlet/wvlet)
- [PyPI Package](https://pypi.org/project/wvlet/)
- [Issue Tracker](https://github.com/wvlet/wvlet/issues)
