-- order_items: independent of daily_orders, so the two jobs can run at once.

CREATE OR REPLACE TABLE `{{ project }}.{{ dataset }}.stg_order_items`
PARTITION BY order_date
AS
SELECT
    DATE(created_at) AS order_date,
    status,
    COUNT(*) AS items,
    SUM(sale_price) AS revenue
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE created_at >= TIMESTAMP(@run_date)
GROUP BY 1, 2;
