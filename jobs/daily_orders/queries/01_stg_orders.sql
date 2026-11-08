-- stg_orders: level 0, parallel with stg_users.
--
-- {{ project }} / {{ dataset }} come from the runner. @run_date is a real
-- BigQuery query parameter (pass it with --var run_date=2024-01-01).
-- Idempotent: full replace, so re-running is safe.

CREATE OR REPLACE TABLE `{{ project }}.{{ dataset }}.stg_orders`
PARTITION BY DATE(created_at)
AS
SELECT
    order_id,
    user_id,
    status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE created_at >= TIMESTAMP(@run_date);
