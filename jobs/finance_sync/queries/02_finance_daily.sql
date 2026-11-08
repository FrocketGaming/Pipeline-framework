-- finance_daily: depends on order_items.

CREATE OR REPLACE TABLE `{{ project }}.{{ dataset }}.finance_daily`
AS
SELECT
    order_date,
    COUNTIF(status = 'complete') AS completed_items,
    SUM(revenue) AS revenue,
    SAFE_DIVIDE(SUM(revenue), COUNT(*)) AS revenue_per_item
FROM `{{ project }}.{{ dataset }}.stg_order_items`
GROUP BY 1;
