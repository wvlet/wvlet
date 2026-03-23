-- Q2: Multi-table Join with Derived Column
-- Business question: For each order, show customer name, product name, and line total with discount.
SELECT
  c.name AS customer_name,
  p.name AS product_name,
  o.quantity,
  o.unit_price,
  o.quantity * o.unit_price * (1 - COALESCE(o.discount, 0)) AS line_total
FROM (VALUES
  (1, 101, 201, 2, 50.00, 0.1, '2025-03-01'),
  (2, 102, 202, 1, 30.00, null, '2025-02-15'),
  (3, 101, 203, 3, 20.00, 0.2, '2025-03-10'),
  (4, 103, 201, 1, 50.00, 0.0, '2024-12-01')
) AS o(order_id, customer_id, product_id, quantity, unit_price, discount, order_date)
JOIN (VALUES
  (101, 'Alice'),
  (102, 'Bob'),
  (103, 'Carol')
) AS c(id, name) ON o.customer_id = c.id
JOIN (VALUES
  (201, 'Widget'),
  (202, 'Gadget'),
  (203, 'Gizmo')
) AS p(id, name) ON o.product_id = p.id
WHERE o.order_date >= '2025-01-01'
ORDER BY line_total DESC;
