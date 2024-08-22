
SELECT to_char(dr.order_date::timestamp, 'yyyy-MM') AS order_month,
	to_char(dr.order_date::timestamp, 'yyyy-MM-dd') AS order_date,
	dr.order_revenue,
	SUM(dr.order_revenue) OVER(
	PARTITION BY to_char(dr.order_date::timestamp, 'yyyy-MM')
	) AS monthly_order_revenue
FROM daily_revenue AS dr
ORDER BY 2;






SELECT dr.*,
	SUM(dr.order_revenue) OVER(
	PARTITION BY 1
	) AS total_order_revenue
FROM daily_revenue AS dr
ORDER BY 1;


