/* PRODUCT PERFORMANCE (Hardcoded order tables) */

WITH all_orders AS (
    SELECT order_id, order_date, product_id, qty, unit_price, order_status
    FROM order_20251025
    UNION ALL
    SELECT order_id, order_date, product_id, qty, unit_price, order_status
    FROM order_20251026
),

product_perf AS (
  SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(CASE WHEN o.order_status = 'completed' THEN o.qty ELSE 0 END) AS units_completed,
    SUM(CASE WHEN o.order_status = 'returned' THEN o.qty ELSE 0 END) AS units_returned,
    SUM(CASE WHEN o.order_status = 'completed' THEN o.qty * o.unit_price ELSE 0 END) AS revenue_completed,
    SUM(CASE WHEN o.order_status = 'returned' THEN o.qty * o.unit_price ELSE 0 END) AS revenue_returned
  FROM all_orders o
  LEFT JOIN products p ON o.product_id = p.product_id
  GROUP BY p.product_id, p.product_name, p.category
)

SELECT
  product_id,
  product_name,
  category,
  (units_completed + units_returned) AS total_units_transacted,
  units_completed AS units_sold,
  units_returned AS units_returned,
  ROUND(revenue_completed,2) AS revenue_completed,
  ROUND(revenue_returned,2) AS revenue_returned,
  CASE WHEN (units_completed + units_returned) = 0 THEN 0
       ELSE ROUND((units_returned / (units_completed + units_returned)) * 100, 2)
  END AS return_rate_percent
FROM product_perf
ORDER BY revenue_completed DESC;
