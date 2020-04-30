-- note there could be 2 payments for one order

{{ config(materialized='table') }}


WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),
stg_payments AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
),
final AS (
    SELECT stg_orders.order_id, 
           stg_orders.customer_id, 
           stg_payments.amount_australian_dollar
    FROM stg_orders
    LEFT JOIN stg_payments
    ON stg_payments.order_id = stg_orders.order_id
)
SELECT *
FROM final