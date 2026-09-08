-- Top products by units sold and total sales

SELECT
    product_name,
    category,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS total_sales
FROM processed
GROUP BY product_name, category
ORDER BY units_sold DESC, total_sales DESC;