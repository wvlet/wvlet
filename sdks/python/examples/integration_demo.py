#!/usr/bin/env python3
"""
Integration examples showing how to use Wvlet with popular Python data tools.

This demonstrates integration with pandas, DuckDB, SQLAlchemy, and other
common data processing libraries.
"""

from wvlet import compile
import os
import tempfile


def duckdb_integration():
    """Show integration with DuckDB for in-memory analytics."""
    print("=== DuckDB Integration ===")
    
    try:
        import duckdb
        import pandas as pd
    except ImportError:
        print("Please install duckdb and pandas: pip install duckdb pandas")
        return
    
    # Create sample data
    conn = duckdb.connect(':memory:')
    
    # Create sample sales data
    conn.execute("""
        CREATE TABLE sales AS 
        SELECT 
            'Product ' || (i % 5 + 1) as product,
            '2024-' || LPAD(CAST((i % 12 + 1) AS VARCHAR), 2, '0') || '-01'::DATE as date,
            (100 + i * 10 + RANDOM() * 50)::DECIMAL(10,2) as amount,
            'Region ' || (i % 3 + 1) as region
        FROM generate_series(1, 100) as t(i)
    """)
    
    # Compile Wvlet query
    wvlet_query = """
    from sales
    group by region, product
    agg 
        sum(amount) as total_sales,
        avg(amount) as avg_sale,
        count(*) as num_sales
    order by region, total_sales desc
    """
    
    sql = compile(wvlet_query, target="duckdb")
    print(f"Generated SQL:\n{sql}\n")
    
    # Execute and fetch results
    df = conn.execute(sql).fetchdf()
    print(f"Results:\n{df.head(10)}")
    print()
    
    # More complex example with window functions
    wvlet_query = """
    model MonthlySales = {
        from sales
        group by date_trunc('month', date) as month, product
        agg sum(amount) as monthly_total
    }
    
    from MonthlySales
    select 
        month,
        product,
        monthly_total,
        sum(monthly_total) over (
            partition by product 
            order by month 
            rows between 2 preceding and current row
        ) as rolling_3month_total
    order by product, month
    """
    
    sql = compile(wvlet_query, target="duckdb")
    df = conn.execute(sql).fetchdf()
    print(f"Monthly sales with rolling totals:\n{df.head()}")
    
    conn.close()


def pandas_workflow():
    """Demonstrate a pandas-based workflow with Wvlet."""
    print("\n=== Pandas Workflow ===")
    
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        print("Please install pandas and numpy: pip install pandas numpy")
        return
    
    # Create sample DataFrames
    users = pd.DataFrame({
        'user_id': range(1, 101),
        'name': [f'User {i}' for i in range(1, 101)],
        'signup_date': pd.date_range('2024-01-01', periods=100, freq='D'),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100)
    })
    
    orders = pd.DataFrame({
        'order_id': range(1, 501),
        'user_id': np.random.randint(1, 101, 500),
        'order_date': pd.date_range('2024-01-01', periods=500, freq='6H'),
        'amount': np.random.uniform(10, 500, 500).round(2)
    })
    
    # Save to temporary parquet files
    with tempfile.TemporaryDirectory() as tmpdir:
        users_path = os.path.join(tmpdir, 'users.parquet')
        orders_path = os.path.join(tmpdir, 'orders.parquet')
        
        users.to_parquet(users_path)
        orders.to_parquet(orders_path)
        
        # Use Wvlet to generate analysis query
        wvlet_query = f"""
        model UserStats = {
            from '{orders_path}' o
            group by user_id
            agg 
                count(*) as order_count,
                sum(amount) as total_spent,
                avg(amount) as avg_order_value,
                max(order_date) as last_order_date
        }
        
        from '{users_path}' u
        left join UserStats s on u.user_id = s.user_id
        group by u.region
        agg 
            count(distinct u.user_id) as total_users,
            count(distinct case when s.order_count > 0 then u.user_id end) as active_users,
            sum(coalesce(s.total_spent, 0)) as revenue,
            avg(coalesce(s.order_count, 0)) as avg_orders_per_user
        order by revenue desc
        """
        
        sql = compile(wvlet_query, target="duckdb")
        print(f"Analysis query:\n{sql}\n")
        
        # Execute with DuckDB on parquet files
        try:
            import duckdb
            result = duckdb.sql(sql).fetchdf()
            print(f"Regional analysis results:\n{result}")
        except ImportError:
            print("Install duckdb to execute: pip install duckdb")


def sqlalchemy_example():
    """Show how to use Wvlet with SQLAlchemy."""
    print("\n=== SQLAlchemy Integration ===")
    
    try:
        from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Date
        from sqlalchemy.orm import declarative_base
    except ImportError:
        print("Please install sqlalchemy: pip install sqlalchemy")
        return
    
    # Create in-memory SQLite database
    engine = create_engine('sqlite:///:memory:', echo=False)
    
    # Create sample tables
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                price REAL
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                quantity INTEGER,
                order_date DATE
            )
        """))
        
        # Insert sample data
        conn.execute(text("""
            INSERT INTO products (id, name, category, price) VALUES
            (1, 'Laptop', 'Electronics', 999.99),
            (2, 'Mouse', 'Electronics', 29.99),
            (3, 'Desk', 'Furniture', 299.99),
            (4, 'Chair', 'Furniture', 199.99),
            (5, 'Monitor', 'Electronics', 399.99)
        """))
        
        conn.execute(text("""
            INSERT INTO orders (product_id, quantity, order_date) VALUES
            (1, 2, '2024-01-15'),
            (2, 5, '2024-01-16'),
            (1, 1, '2024-01-17'),
            (3, 1, '2024-01-18'),
            (4, 2, '2024-01-18'),
            (5, 3, '2024-01-19')
        """))
        
        conn.commit()
    
    # Use Wvlet to generate analysis query
    wvlet_query = """
    from orders o
    join products p on o.product_id = p.id
    group by p.category
    agg 
        sum(o.quantity * p.price) as revenue,
        sum(o.quantity) as units_sold,
        count(distinct o.id) as num_orders
    order by revenue desc
    """
    
    # Note: SQLite doesn't have a specific target in Wvlet, using default
    sql = compile(wvlet_query)
    print(f"Revenue by category query:\n{sql}\n")
    
    # Execute query
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        print("Results:")
        print(f"{'Category':<15} {'Revenue':<12} {'Units':<8} {'Orders':<8}")
        print("-" * 45)
        for row in result:
            print(f"{row[0]:<15} ${row[1]:<11.2f} {row[2]:<8} {row[3]:<8}")


def streaming_analytics_pattern():
    """Show a pattern for streaming analytics with Wvlet."""
    print("\n=== Streaming Analytics Pattern ===")
    
    # Example of generating SQL for windowed aggregations
    # that could be used with streaming systems
    
    wvlet_query = """
    model EventStream = {
        from events
        where event_time > current_timestamp - interval '1 hour'
    }
    
    from EventStream
    group by date_trunc('minute', event_time) as minute, event_type
    agg 
        count(*) as event_count,
        count(distinct user_id) as unique_users,
        avg(response_time) as avg_response_time,
        percentile_cont(0.95) within group (order by response_time) as p95_response_time
    order by minute desc, event_count desc
    """
    
    sql = compile(wvlet_query)
    print("Streaming analytics SQL (for last hour):")
    print(sql)
    print("\nThis SQL can be used with streaming databases like:")
    print("- Apache Flink SQL")
    print("- ksqlDB")
    print("- Materialize")
    print("- RisingWave")


def batch_processing_example():
    """Example of using Wvlet for batch processing workflows."""
    print("\n=== Batch Processing Example ===")
    
    # Generate SQL for a typical ETL/batch processing task
    wvlet_query = """
    -- Extract and transform user activity data
    model DailyUserActivity = {
        from raw_events
        where date(event_time) = current_date - 1
        group by user_id, date(event_time) as activity_date
        agg 
            count(*) as event_count,
            count(distinct session_id) as session_count,
            sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_count,
            sum(case when event_type = 'purchase' then event_value else 0 end) as revenue
    }
    
    -- Merge with user dimensions
    model EnrichedActivity = {
        from DailyUserActivity a
        left join users u on a.user_id = u.user_id
        select 
            a.*,
            u.segment,
            u.acquisition_channel,
            u.country
    }
    
    -- Final aggregated metrics
    from EnrichedActivity
    group by activity_date, segment, country
    agg 
        count(distinct user_id) as active_users,
        sum(event_count) as total_events,
        sum(purchase_count) as total_purchases,
        sum(revenue) as total_revenue,
        sum(revenue) / nullif(sum(purchase_count), 0) as avg_order_value
    """
    
    sql = compile(wvlet_query)
    print("Batch processing SQL:")
    print(sql)
    print("\nThis SQL can be scheduled in:")
    print("- Apache Airflow")
    print("- dbt")
    print("- Dagster")
    print("- Prefect")


def main():
    """Run all integration examples."""
    print("Wvlet Integration Examples")
    print("=" * 50)
    
    duckdb_integration()
    pandas_workflow()
    sqlalchemy_example()
    streaming_analytics_pattern()
    batch_processing_example()
    
    print("\n" + "=" * 50)
    print("Integration examples completed!")
    print("\nThese examples show how Wvlet can be integrated into various")
    print("Python data workflows and tools. The generated SQL can be used")
    print("with any SQL-compatible database or processing engine.")


if __name__ == "__main__":
    main()