-- Q3: Window Functions — Ranking + Running Total
-- Business question: For each customer, rank orders by amount and show running cumulative total.
SELECT
  customer_id,
  order_id,
  amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rank,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_total
FROM (VALUES
  (1, 'C001', 100, 'completed', '2025-01-10'),
  (2, 'C001', 250, 'completed', '2025-01-15'),
  (3, 'C001', 150, 'completed', '2025-01-20'),
  (4, 'C002', 300, 'completed', '2025-01-12'),
  (5, 'C002', 200, 'completed', '2025-01-18'),
  (6, 'C002', 100, 'pending',   '2025-01-25')
) AS orders(order_id, customer_id, amount, status, order_date)
WHERE status = 'completed'
ORDER BY customer_id, rank;
