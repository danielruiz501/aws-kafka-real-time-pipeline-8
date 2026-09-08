-- Sales performance by order status

SELECT
    order_status,
    COUNT(*) AS total_orders,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS total_sales
FROM processed
GROUP BY order_status
ORDER BY total_sales DESC;