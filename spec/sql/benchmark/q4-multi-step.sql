-- Q4: Multi-step Aggregation — Aggregate → Classify → Re-aggregate
-- Business question: Classify customers into tiers based on purchase behavior, then summarize tiers.
WITH recent_purchases AS (
  SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(amount) AS total_spend
  FROM (VALUES
    (1, 'C001', 500, 'completed', '2025-01-05'),
    (2, 'C001', 800, 'completed', '2025-01-10'),
    (3, 'C001', 200, 'completed', '2025-01-15'),
    (4, 'C002', 3000, 'completed', '2025-01-08'),
    (5, 'C002', 4000, 'completed', '2025-01-12'),
    (6, 'C002', 3500, 'completed', '2025-01-20'),
    (7, 'C003', 150, 'completed', '2025-01-03'),
    (8, 'C004', 6000, 'completed', '2025-01-02'),
    (9, 'C004', 5000, 'completed', '2025-01-09'),
    (10, 'C004', 4500, 'cancelled', '2025-01-15')
  ) AS orders(order_id, customer_id, amount, status, order_date)
  WHERE status = 'completed'
  GROUP BY customer_id
),
tiered_customers AS (
  SELECT
    customer_id,
    purchase_count,
    total_spend,
    CASE
      WHEN total_spend >= 10000 AND purchase_count >= 3 THEN 'platinum'
      WHEN total_spend >= 5000 AND purchase_count >= 2 THEN 'gold'
      WHEN total_spend >= 1000 THEN 'silver'
      ELSE 'bronze'
    END AS tier
  FROM recent_purchases
)
SELECT
  tier,
  COUNT(*) AS customer_count,
  AVG(total_spend) AS avg_spend
FROM tiered_customers
GROUP BY tier
ORDER BY
  CASE tier
    WHEN 'platinum' THEN 1
    WHEN 'gold' THEN 2
    WHEN 'silver' THEN 3
    ELSE 4
  END;
