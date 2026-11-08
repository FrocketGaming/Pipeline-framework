-- fact_orders: level 1, waits for both staging steps (fan-in).
--
-- CREATE OR REPLACE makes the publish atomic: consumers see the previous table
-- until this statement commits.

CREATE OR REPLACE TABLE `{{ project }}.{{ dataset }}.fact_orders`
PARTITION BY order_date
CLUSTER BY country
AS
SELECT
    DATE(o.created_at) AS order_date,
    COALESCE(u.country, 'UNKNOWN') AS country,
    o.status,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.num_of_item) AS items,
    COUNT(DISTINCT o.user_id) AS buyers
FROM `{{ project }}.{{ dataset }}.stg_orders` AS o
LEFT JOIN `{{ project }}.{{ dataset }}.stg_users` AS u
    ON o.user_id = u.user_id
GROUP BY 1, 2, 3;
