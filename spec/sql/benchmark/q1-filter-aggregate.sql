-- Q1: Simple Filter + Aggregate
-- Business question: How many active orders per customer, with total spend?
SELECT
  customer_id,
  COUNT(*) AS order_count,
  SUM(amount) AS total_spend
FROM (VALUES
  (1, 'C001', 'active',  500),
  (2, 'C001', 'active',  800),
  (3, 'C002', 'active',  300),
  (4, 'C002', 'active',  900),
  (5, 'C003', 'active',  200),
  (6, 'C003', 'cancelled', 400),
  (7, 'C001', 'active', 1200),
  (8, 'C004', 'active',  100)
) AS orders(order_id, customer_id, status, amount)
WHERE status = 'active'
GROUP BY customer_id
HAVING SUM(amount) > 1000
ORDER BY total_spend DESC;
