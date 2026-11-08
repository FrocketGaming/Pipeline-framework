-- stg_users: level 0, parallel with stg_orders.

CREATE OR REPLACE TABLE `{{ project }}.{{ dataset }}.stg_users`
AS
SELECT
    id AS user_id,
    first_name,
    last_name,
    gender,
    age,
    state,
    country,
    traffic_source,
    created_at
FROM `bigquery-public-data.thelook_ecommerce.users`;
