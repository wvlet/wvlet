# Python SDK

The Wvlet Python SDK provides a native Python interface for compiling Wvlet queries to SQL. It offers high-performance compilation through a bundled native library while maintaining a simple, Pythonic API.

## Features

- 🚀 **Fast native compilation** - Uses bundled native library for high-performance query compilation
- 🔄 **Automatic fallback** - Falls back to CLI when native library is unavailable
- 🎯 **Multiple SQL targets** - Supports DuckDB, Trino, and other SQL engines
- 📦 **Zero dependencies** - Pure Python with optional native acceleration
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
model UserStats =
  from users
  select user_id, count(*) as event_count
  group by user_id

from UserStats
where event_count > 100
""")
```

## Basic Usage

### Simple Compilation

The simplest way to use Wvlet is through the `compile` function:

```python
from wvlet import compile

# Basic select
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

For more control, use the `WvletCompiler` class:

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

## Advanced Features

### Models (CTEs)

Models in Wvlet are like CTEs but more intuitive:

```python
sql = compile("""
model ActiveUsers = 
  from users 
  where last_login > current_date - interval '30' day
  select user_id, email

model UserOrders =
  from orders o
  join ActiveUsers u on o.user_id = u.user_id
  select o.*, u.email

from UserOrders
select email, count(*) as order_count
group by email
""")
```

### Window Functions

```python
sql = compile("""
from sales
select 
  date,
  amount,
  sum(amount) over (order by date rows 7 preceding) as rolling_7day_sum,
  rank() over (partition by product_id order by amount desc) as rank_by_product
""")
```

### Pivot Operations

```python
sql = compile("""
from sales
pivot sum(amount) for category in ('Electronics', 'Clothing', 'Food')
group by date
""")
```

## Integration Examples

### With DuckDB

```python
import duckdb
from wvlet import compile

# Compile Wvlet to SQL
wvlet_query = """
from 'sales.parquet'
where region = 'North America'
select 
  date_trunc('month', date) as month,
  sum(amount) as total_sales
group by date_trunc('month', date)
order by month
"""

sql = compile(wvlet_query, target="duckdb")

# Execute with DuckDB
conn = duckdb.connect()
df = conn.execute(sql).fetchdf()
```

### With Pandas

```python
import pandas as pd
import numpy as np
import duckdb
from wvlet import compile

# Create sample data
df = pd.DataFrame({
    'user_id': range(1, 101),
    'score': np.random.randint(0, 100, 100),
    'category': np.random.choice(['A', 'B', 'C'], 100)
})

# Save to parquet
df.to_parquet('users.parquet')

# Analyze with Wvlet
sql = compile("""
from 'users.parquet'
select 
  category,
  avg(score) as avg_score,
  count(*) as user_count
group by category
""", target="duckdb")

result = duckdb.sql(sql).fetchdf()
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
select c.name, sum(o.total) as weekly_total
group by c.name
having sum(o.total) > 1000
"""

sql = compile(wvlet_query)  # Uses default SQL dialect

with engine.connect() as conn:
    result = conn.execute(text(sql))
    for row in result:
        print(row)
```

## Error Handling

```python
from wvlet import compile
from wvlet.compiler import CompilationError

try:
    sql = compile("invalid query syntax")
except CompilationError as e:
    print(f"Compilation failed: {e}")
    # Access detailed error information if available
    if hasattr(e, 'line'):
        print(f"Error at line {e.line}: {e.message}")
```

## Performance Considerations

The native library provides significant performance improvements:

```python
import time
from wvlet import compile

# Benchmark native compilation
start = time.time()
for _ in range(100):
    compile("from users select * where age > 21")
print(f"Native: {time.time() - start:.2f}s")

# Force CLI usage (if available)
import os
os.environ['WVLET_PYTHON_USE_CLI'] = '1'
start = time.time()
for _ in range(100):
    compile("from users select * where age > 21")
print(f"CLI: {time.time() - start:.2f}s")
```

## Platform Support

### Native Library Availability

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux    | x86_64      | ✅ Supported |
| Linux    | aarch64     | ✅ Supported |
| macOS    | arm64       | ✅ Supported |
| macOS    | x86_64      | 🔄 Planned |
| Windows  | x86_64      | 🔄 Planned |

The SDK automatically falls back to the CLI if the native library is unavailable.

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
- `NotImplementedError`: If neither native library nor CLI is available

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

## Troubleshooting

### Native Library Not Found

If you see an error about the native library not being found:

1. Check your platform is supported (see Platform Support above)
2. Ensure you have the latest version: `pip install --upgrade wvlet`
3. Try reinstalling: `pip install --force-reinstall wvlet`
4. Use CLI fallback by installing the Wvlet CLI

### Compilation Errors

For query compilation errors:

1. Check query syntax matches [Wvlet syntax guide](/docs/syntax/)
2. Use the CLI to validate: `wvlet compile "your query"`
3. Enable debug logging:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

## Next Steps

- Explore the [example scripts](https://github.com/wvlet/wvlet/tree/main/sdks/python/examples) on GitHub
- Read the [Wvlet syntax guide](/docs/syntax/) to learn more query patterns
- Try the [interactive tutorial notebook](https://github.com/wvlet/wvlet/blob/main/sdks/python/examples/wvlet_tutorial.ipynb)
- Join the [Wvlet community](https://github.com/wvlet/wvlet/discussions) for support
