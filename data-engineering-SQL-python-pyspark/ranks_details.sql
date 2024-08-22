SELECT COUNT(*) FROM daily_product_revenue;

SELECT * FROM daily_product_revenue
ORDER BY order_date, order_revenue DESC;


-- rank()  -> OVER()
-- dense_rank() -> OVER(PARTITION BY )

-- Global Ranking - > rank OVER(ORDER BY col1 DESC)
-- Ranking based on key or Partition key - > rank() OVER(PARTITION BY col2 ORDER BY col1 DESC)


SELECT order_date,
order_item_product_id,
order_revenue,
RANK() OVER(PARTITION BY order_date ORDER BY order_revenue DESC) AS rnk,
dense_rank() OVER(PARTITION BY order_date ORDER BY order_revenue DESC) AS drnk
FROM daily_product_revenue
WHERE to_char(order_date::date,'yyyy-MM')='2014-01'
ORDER BY order_revenue DESC;
