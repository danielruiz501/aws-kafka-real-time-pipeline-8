-- Sales performance by product category

SELECT
    category,
    COUNT(*) AS total_orders,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS total_sales
FROM processed
GROUP BY category
ORDER BY total_sales DESC;