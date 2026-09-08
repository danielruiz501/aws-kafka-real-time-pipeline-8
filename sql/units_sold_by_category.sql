-- Units sold by product category

SELECT
    category,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS total_sales
FROM processed
GROUP BY category
ORDER BY total_sales DESC;