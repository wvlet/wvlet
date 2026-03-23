-- Q5: Multi-Source Reconciliation
-- Business question: Find revenue discrepancies between orders and payments systems.
-- Note: The subtotal expression SUM(quantity * unit_price * (1 - COALESCE(discount, 0)))
-- would be repeated 3 times in a real SQL query (for subtotal, tax_amount, expected_total).
-- This version uses pre-computed totals for clarity.
WITH order_totals(order_id, customer_id, expected_total) AS (
  VALUES
    (1, 'C001', 145.80),
    (2, 'C002', 69.00),
    (3, 'C003', 102.60),
    (4, 'C004', 96.00)
),
payment_totals(pt_order_id, paid_total, payment_count) AS (
  VALUES
    (1, 145.00, 1),
    (2, 60.00, 1),
    (3, 102.60, 1),
    (4, 90.00, 1)
)
SELECT
  ot.order_id,
  ot.customer_id,
  ot.expected_total,
  COALESCE(pt.paid_total, 0) AS paid,
  ot.expected_total - COALESCE(pt.paid_total, 0) AS discrepancy,
  CASE
    WHEN pt.pt_order_id IS NULL THEN 'no_payment'
    WHEN ABS(ot.expected_total - pt.paid_total) < 0.01 THEN 'match'
    WHEN pt.paid_total < ot.expected_total THEN 'underpaid'
    WHEN pt.paid_total > ot.expected_total THEN 'overpaid'
  END AS discrepancy_type
FROM order_totals ot
LEFT JOIN payment_totals pt ON ot.order_id = pt.pt_order_id
WHERE CASE
    WHEN pt.pt_order_id IS NULL THEN 'no_payment'
    WHEN ABS(ot.expected_total - pt.paid_total) < 0.01 THEN 'match'
    WHEN pt.paid_total < ot.expected_total THEN 'underpaid'
    WHEN pt.paid_total > ot.expected_total THEN 'overpaid'
  END != 'match'
ORDER BY ABS(ot.expected_total - COALESCE(pt.paid_total, 0)) DESC;
