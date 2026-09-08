-- Sales performance by country

SELECT
    country,
    COUNT(*) AS total_orders,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS total_sales
FROM processed
GROUP BY country
ORDER BY total_sales DESC;