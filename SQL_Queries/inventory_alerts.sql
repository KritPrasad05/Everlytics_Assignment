SELECT
  i.product_id,
  p.product_name,
  i.warehouse_id,
  i.stock_on_hand,
  i.last_restock_date
FROM inventory i
LEFT JOIN products p ON i.product_id = p.product_id
WHERE i.stock_on_hand < 50
ORDER BY i.stock_on_hand ASC;
