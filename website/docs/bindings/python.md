# Python SDK

The Wvlet Python SDK provides a native Python interface for compiling Wvlet queries to SQL. It offers high-performance compilation through a bundled native library while maintaining a simple, Pythonic API.

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
  select user_id, count(*) as event_count
  group by user_id
}

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
model ActiveUsers = {
  from users 
  where last_login > current_date - interval '30' day
  select user_id, email
}

model UserOrders = {
  from orders o
  join ActiveUsers u on o.user_id = u.user_id
  select o.*, u.email
}

from UserOrders
group by email
agg count(*) as order_count
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
group by date
pivot sum(amount) for category in ('Electronics', 'Clothing', 'Food')
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
group by date_trunc('month', date) as month
agg sum(amount) as total_sales
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
group by category
agg 
  avg(score) as avg_score,
  count(*) as user_count
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
group by c.name
agg sum(o.total) as weekly_total
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
```

## Platform Support

### Native Library Availability

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux    | x86_64      | ✅ Supported |
| Linux    | aarch64     | ✅ Supported |
| macOS    | arm64       | ✅ Supported |
| Windows  | x86_64      | ✅ Supported |
| Windows  | arm64       | ✅ Supported |


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

## Troubleshooting

### Native Library Not Found

If you see an error about the native library not being found:

1. Check your platform is supported (see Platform Support above)
2. Ensure you have the latest version: `pip install --upgrade wvlet`
3. Try reinstalling: `pip install --force-reinstall wvlet`

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
