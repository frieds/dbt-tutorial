WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),
stg_payments AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
),
payment_orders AS (
    SELECT stg_payments.order_id, 
           SUM(stg_payments.amount_usd_cents) AS amount_usd_cents
    FROM stg_payments
    GROUP BY 1 
)
final AS (
    SELECT payment_orders.order_id, 
           stg_orders.customer_id, 
           stg_orders.order_date,
           COALESCE(payment_orders.amount_usd_cents, 0) AS amount_usd_cents
    FROM payment_orders
    LEFT JOIN stg_orders
    USING (order_id)
)
SELECT *
FROM final