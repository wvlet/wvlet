#!/usr/bin/env python3
"""
Basic Wvlet Python SDK usage examples.

This script demonstrates the fundamental features of the Wvlet Python SDK,
including query compilation, different SQL targets, and error handling.
"""

from wvlet import compile
from wvlet.compiler import WvletCompiler, CompilationError


def basic_compilation():
    """Demonstrate basic query compilation."""
    print("=== Basic Compilation ===")
    
    # Simple select query
    query = "from employees select name, department, salary"
    sql = compile(query)
    print(f"Wvlet: {query}")
    print(f"SQL:   {sql}")
    print()
    
    # Query with WHERE clause
    query = "from employees where salary > 50000 select name, salary"
    sql = compile(query)
    print(f"Wvlet: {query}")
    print(f"SQL:   {sql}")
    print()


def aggregation_examples():
    """Show aggregation queries."""
    print("=== Aggregation Examples ===")
    
    # Basic aggregation
    query = """
    from sales
    group by product_category
    agg 
        count(*) as num_sales,
        sum(amount) as total_revenue,
        avg(amount) as avg_sale
    """
    sql = compile(query)
    print(f"Aggregation query:")
    print(f"SQL: {sql}")
    print()
    
    # Aggregation with HAVING
    query = """
    from orders
    group by customer_id
    agg count(*) as order_count
    having count(*) > 10
    """
    sql = compile(query)
    print(f"With HAVING clause:")
    print(f"SQL: {sql}")
    print()


def join_examples():
    """Demonstrate JOIN operations."""
    print("=== JOIN Examples ===")
    
    # Inner join
    query = """
    from orders o
    join customers c on o.customer_id = c.id
    select o.order_id, c.name, o.total_amount
    """
    sql = compile(query)
    print(f"Inner join:")
    print(f"SQL: {sql}")
    print()
    
    # Multiple joins
    query = """
    from orders o
    join customers c on o.customer_id = c.id
    join products p on o.product_id = p.id
    select 
        o.order_id,
        c.name as customer_name,
        p.name as product_name,
        o.quantity,
        o.total_amount
    """
    sql = compile(query)
    print(f"Multiple joins:")
    print(f"SQL: {sql}")
    print()


def model_examples():
    """Show how to use models (CTEs)."""
    print("=== Model Examples ===")
    
    # Simple model
    query = """
    model HighValueOrders = {
        from orders
        where total_amount > 1000
    }
    
    from HighValueOrders
    select count(*) as high_value_count
    """
    sql = compile(query)
    print(f"Simple model:")
    print(f"SQL: {sql}")
    print()
    
    # Multiple models
    query = """
    model RecentOrders = {
        from orders
        where order_date > current_date - 30
    }
    
    model CustomerStats = {
        from RecentOrders
        group by customer_id
        agg 
            count(*) as recent_order_count,
            sum(total_amount) as recent_total
    }
    
    from CustomerStats
    where recent_order_count > 5
    """
    sql = compile(query)
    print(f"Multiple models:")
    print(f"SQL: {sql}")
    print()


def window_function_examples():
    """Demonstrate window functions."""
    print("=== Window Function Examples ===")
    
    # Row number
    query = """
    from employees
    select 
        name,
        department,
        salary,
        row_number() over (partition by department order by salary desc) as rank_in_dept
    """
    sql = compile(query)
    print(f"Row number:")
    print(f"SQL: {sql}")
    print()
    
    # Running total
    query = """
    from daily_sales
    select 
        date,
        amount,
        sum(amount) over (order by date) as running_total,
        avg(amount) over (order by date rows between 6 preceding and current row) as avg_7days
    """
    sql = compile(query)
    print(f"Running calculations:")
    print(f"SQL: {sql}")
    print()


def target_specific_compilation():
    """Show compilation for different SQL targets."""
    print("=== Target-Specific Compilation ===")
    
    query = "from users select name, created_at"
    
    # Compile for different targets
    for target in ["duckdb", "trino"]:
        try:
            compiler = WvletCompiler(target=target)
            sql = compiler.compile(query)
            print(f"Target: {target}")
            print(f"SQL:   {sql}")
            print()
        except Exception as e:
            print(f"Error compiling for {target}: {e}")
            print()


def error_handling():
    """Demonstrate error handling."""
    print("=== Error Handling ===")
    
    # Invalid syntax
    try:
        sql = compile("from invalid syntax query")
    except CompilationError as e:
        print(f"Compilation error caught: {e}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
    
    # Missing table reference
    try:
        sql = compile("select * where id = 1")
    except CompilationError as e:
        print(f"Missing FROM clause: {e}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")


def main():
    """Run all examples."""
    print("Wvlet Python SDK Examples")
    print("=" * 50)
    print()
    
    basic_compilation()
    aggregation_examples()
    join_examples()
    model_examples()
    window_function_examples()
    target_specific_compilation()
    error_handling()
    
    print("\nExamples completed!")


if __name__ == "__main__":
    main()