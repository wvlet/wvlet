#!/usr/bin/env python3
"""
Data analysis workflow using Wvlet Python SDK.

This example demonstrates how to use Wvlet for common data analysis tasks,
including data transformation, aggregation, and analytical queries.
"""

from wvlet import compile


def sales_analysis():
    """Analyze sales data with various transformations."""
    print("=== Sales Analysis Example ===")
    
    # Monthly sales trend
    query = """
    model MonthlySales = {
        from sales
        group by date_trunc('month', sale_date) as month
        agg 
            sum(amount) as total_sales,
            count(*) as num_transactions,
            count(distinct customer_id) as unique_customers
    }
    
    from MonthlySales
    select 
        month,
        total_sales,
        num_transactions,
        unique_customers,
        total_sales / nullif(unique_customers, 0) as avg_revenue_per_customer
    order by month
    """
    
    sql = compile(query)
    print("Monthly sales trend SQL:")
    print(sql)
    print()
    
    # Year-over-year comparison
    query = """
    model CurrentYearSales = {
        from sales
        where year(sale_date) = year(current_date)
        group by month(sale_date) as month
        agg sum(amount) as sales_current
    }
    
    model LastYearSales = {
        from sales
        where year(sale_date) = year(current_date) - 1
        group by month(sale_date) as month
        agg sum(amount) as sales_last_year
    }
    
    from CurrentYearSales c
    left join LastYearSales l on c.month = l.month
    select 
        c.month,
        c.sales_current,
        l.sales_last_year,
        (c.sales_current - l.sales_last_year) / nullif(l.sales_last_year, 0) * 100 as yoy_growth_pct
    order by c.month
    """
    
    sql = compile(query)
    print("Year-over-year comparison SQL:")
    print(sql)
    print()


def customer_segmentation():
    """Customer segmentation based on purchase behavior."""
    print("=== Customer Segmentation ===")
    
    query = """
    model CustomerMetrics = {
        from orders
        group by customer_id
        agg 
            count(*) as order_count,
            sum(total_amount) as lifetime_value,
            avg(total_amount) as avg_order_value,
            max(order_date) as last_order_date,
            min(order_date) as first_order_date
    }
    
    from CustomerMetrics
    select 
        customer_id,
        lifetime_value,
        order_count,
        case 
            when lifetime_value > 10000 then 'VIP'
            when lifetime_value > 5000 then 'High Value'
            when lifetime_value > 1000 then 'Regular'
            else 'Low Value'
        end as customer_segment,
        case
            when last_order_date > current_date - 30 then 'Active'
            when last_order_date > current_date - 90 then 'At Risk'
            else 'Churned'
        end as customer_status
    """
    
    sql = compile(query)
    print("Customer segmentation SQL:")
    print(sql)
    print()


def product_performance():
    """Analyze product performance with advanced metrics."""
    print("=== Product Performance Analysis ===")
    
    query = """
    model ProductSales = {
        from order_items oi
        join products p on oi.product_id = p.id
        group by p.category, p.name as product_name
        agg 
            sum(oi.quantity) as units_sold,
            sum(oi.quantity * oi.unit_price) as revenue
    }
    
    from ProductSales
    select 
        category,
        product_name,
        units_sold,
        revenue,
        revenue / nullif(sum(revenue) over (partition by category), 0) * 100 as category_revenue_share,
        rank() over (partition by category order by revenue desc) as rank_in_category,
        revenue / nullif(sum(revenue) over (), 0) * 100 as total_revenue_share
    order by category, revenue desc
    """
    
    sql = compile(query)
    print("Product performance SQL:")
    print(sql)
    print()


def cohort_analysis():
    """Perform cohort analysis on user behavior."""
    print("=== Cohort Analysis ===")
    
    query = """
    model UserCohorts = {
        from users
        select 
            user_id,
            date_trunc('month', created_at) as cohort_month
    }
    
    model CohortActivity = {
        from orders o
        join UserCohorts uc on o.user_id = uc.user_id
        group by uc.cohort_month, date_trunc('month', o.order_date) as activity_month
        agg count(distinct o.user_id) as active_users
    }
    
    from CohortActivity
    select 
        cohort_month,
        activity_month,
        active_users,
        datediff('month', cohort_month, activity_month) as months_since_signup
    where datediff('month', cohort_month, activity_month) >= 0
    order by cohort_month, activity_month
    """
    
    sql = compile(query)
    print("Cohort analysis SQL:")
    print(sql)
    print()


def funnel_analysis():
    """Analyze conversion funnel."""
    print("=== Funnel Analysis ===")
    
    query = """
    model FunnelSteps = {
        from events
        group by user_id
        agg 
            max(case when event_name = 'page_view' then 1 else 0 end) as viewed,
            max(case when event_name = 'add_to_cart' then 1 else 0 end) as added_to_cart,
            max(case when event_name = 'checkout' then 1 else 0 end) as checked_out,
            max(case when event_name = 'purchase' then 1 else 0 end) as purchased
    }
    
    from FunnelSteps
    select 
        count(*) as total_users,
        sum(viewed) as viewed_product,
        sum(added_to_cart) as added_to_cart,
        sum(checked_out) as checked_out,
        sum(purchased) as completed_purchase,
        sum(added_to_cart) * 100.0 / nullif(sum(viewed), 0) as view_to_cart_rate,
        sum(checked_out) * 100.0 / nullif(sum(added_to_cart), 0) as cart_to_checkout_rate,
        sum(purchased) * 100.0 / nullif(sum(checked_out), 0) as checkout_to_purchase_rate,
        sum(purchased) * 100.0 / nullif(sum(viewed), 0) as overall_conversion_rate
    """
    
    sql = compile(query)
    print("Funnel analysis SQL:")
    print(sql)
    print()


def time_series_analysis():
    """Time series analysis with moving averages and trends."""
    print("=== Time Series Analysis ===")
    
    query = """
    model DailyMetrics = {
        from events
        where event_date >= current_date - 90
        group by event_date as date
        agg 
            count(distinct user_id) as daily_active_users,
            count(*) as total_events
    }
    
    from DailyMetrics
    select 
        date,
        daily_active_users,
        total_events,
        avg(daily_active_users) over (
            order by date 
            rows between 6 preceding and current row
        ) as dau_7day_avg,
        avg(daily_active_users) over (
            order by date 
            rows between 29 preceding and current row
        ) as dau_30day_avg,
        daily_active_users - lag(daily_active_users, 7) over (order by date) as dau_week_over_week,
        (daily_active_users - lag(daily_active_users, 7) over (order by date)) * 100.0 / 
            nullif(lag(daily_active_users, 7) over (order by date), 0) as dau_wow_pct
    order by date
    """
    
    sql = compile(query)
    print("Time series analysis SQL:")
    print(sql)
    print()


def main():
    """Run all data analysis examples."""
    print("Wvlet Data Analysis Examples")
    print("=" * 50)
    print()
    
    sales_analysis()
    customer_segmentation()
    product_performance()
    cohort_analysis()
    funnel_analysis()
    time_series_analysis()
    
    print("\nData analysis examples completed!")
    print("\nNote: These queries can be executed against your actual database")
    print("using the compiled SQL with your preferred database client or ORM.")


if __name__ == "__main__":
    main()