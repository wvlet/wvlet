# Wvlet Python SDK Examples

This directory contains example scripts demonstrating various features and use cases of the Wvlet Python SDK.

## Examples Overview

### 1. Basic Usage (`basic_usage.py`)
Demonstrates fundamental Wvlet features:
- Simple query compilation
- Aggregations and GROUP BY
- JOIN operations
- Models (CTEs)
- Window functions
- Target-specific compilation
- Error handling

Run with:
```bash
python examples/basic_usage.py
```

### 2. Data Analysis (`data_analysis.py`)
Shows real-world data analysis patterns:
- Sales trend analysis
- Customer segmentation
- Product performance metrics
- Cohort analysis
- Funnel analysis
- Time series analysis with moving averages

Run with:
```bash
python examples/data_analysis.py
```

### 3. Integration Demo (`integration_demo.py`)
Demonstrates integration with popular Python tools:
- **DuckDB**: In-memory analytics with pandas DataFrames
- **Pandas**: Working with parquet files and DataFrames
- **SQLAlchemy**: Database ORM integration
- **Streaming**: Patterns for real-time analytics
- **Batch Processing**: ETL and scheduled job patterns

Run with:
```bash
# Install optional dependencies first
pip install duckdb pandas sqlalchemy

python examples/integration_demo.py
```

## Quick Start

1. Install the Wvlet Python SDK:
```bash
pip install wvlet
```

2. Run any example:
```bash
python examples/basic_usage.py
```

## Creating Your Own Examples

To create a new example using Wvlet:

```python
from wvlet import compile

# Write your Wvlet query
query = """
from users
where created_at > current_date - 30
select user_id, email, created_at
"""

# Compile to SQL
sql = compile(query)
print(sql)

# Use the SQL with your database
# ... execute sql with your preferred database client
```

## Common Patterns

### Using Models for Complex Queries
```python
query = """
model ActiveUsers = {
    from users 
    where last_login > current_date - 7
    select user_id, email
}

from ActiveUsers
select count(*) as weekly_active_users
"""
```

### Aggregations with Window Functions
```python
query = """
from sales
select 
    date,
    amount,
    sum(amount) over (order by date) as running_total,
    rank() over (partition by product_id order by amount desc) as rank_by_product
"""
```

### Multi-step Data Transformations
```python
query = """
model Step1 = { from raw_data select ... }
model Step2 = { from Step1 where ... }
model Step3 = { from Step2 join other_table ... }

from Step3
select final_results
"""
```

## Tips

1. **Use Models**: Break complex queries into logical steps using models
2. **Target-specific SQL**: Specify the target database for optimized SQL
3. **Error Handling**: Always wrap compilation in try-except for production code
4. **Integration**: Wvlet works great with pandas, DuckDB, and other data tools

## Resources

- [Wvlet Documentation](https://wvlet.org/docs/)
- [Wvlet Syntax Guide](https://wvlet.org/docs/syntax/)
- [Python SDK API Reference](https://wvlet.org/docs/bindings/python/)