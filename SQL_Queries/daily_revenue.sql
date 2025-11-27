/* DAILY REVENUE + TOP CATEGORY (Hardcoded tables) */

WITH all_orders AS (
    SELECT order_id, order_date, product_id, qty, unit_price, order_status
    FROM order_20251025
    UNION ALL
    SELECT order_id, order_date, product_id, qty, unit_price, order_status
    FROM order_20251026
),

orders_calc AS (
  SELECT
    DATE(order_date) AS order_date_only,
    product_id,
    qty,
    unit_price,
    CASE 
        WHEN order_status = 'cancelled' THEN 0
        ELSE qty * unit_price
    END AS order_amount
  FROM all_orders
),

daily_cat AS (
  SELECT 
      oc.order_date_only,
      p.category,
      SUM(oc.order_amount) AS revenue
  FROM orders_calc oc
  LEFT JOIN products p ON oc.product_id = p.product_id
  GROUP BY oc.order_date_only, p.category
),

daily_total AS (
  SELECT order_date_only, SUM(revenue) AS total_revenue
  FROM daily_cat
  GROUP BY order_date_only
),

top_cat AS (
  SELECT 
      order_date_only,
      category,
      revenue,
      ROW_NUMBER() OVER (PARTITION BY order_date_only ORDER BY revenue DESC) AS rn
  FROM daily_cat
)

SELECT
  DATE_FORMAT(dt.order_date_only, '%Y-%m-%d') AS date,
  ROUND(dt.total_revenue,2) AS total_revenue,
  tc.category AS top_category,
  ROUND(tc.revenue,2) AS top_category_revenue
FROM daily_total dt
LEFT JOIN top_cat tc 
  ON dt.order_date_only = tc.order_date_only AND tc.rn = 1
ORDER BY dt.order_date_only;
