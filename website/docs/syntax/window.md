# Window Function

Window function computes the data against the whole query result.

- partition by (key): Split the query result into multiple partitions based on the key.
- order by (key): Define ordering of the rows within each partition.
- rows [start:end]: Define the range of rows in each window.

This query calculates the total amount of orders for each customer:
```wvlet
from orders
select sum(amount) over (partition by customer_id order by order_date)
```

## Window Frame 

In Wvlet, you can specify range of rows in each window with the following syntax:

| Syntax | Window Frame in SQL |
| --- | --- |
| rows [,0] | rows between unbounded preceding and current row (default) |
| rows [-1,1] | rows between 1 preceding and 1 following |
| rows [-1,0] | rows between 1 preceding and current row |
| rows [0,1] | rows between current row and 1 following |
| rows [0,] | rows between current row and unbounded following |


`rows [,0]` is the default window frame if no frame is specified. This means the window frame includes all rows from the first row of the partition to the current row. To explicitly specify the window frame, you can use the `rows` keyword as follows:

```wvlet
from orders
select amount.sum() over (
    partition by customer_id
    order by order_date
    rows [,0]
  ) as cumulative_amount
```
