WITH payment_amount_per_order AS (
    SELECT *
    FROM {{ ref('payment_amount_per_order') }}
),
stg_distinct_orders AS (
    SELECT DISTINCT customer_id, order_id
    FROM {{ ref('stg_orders') }}
),
final AS (
    SELECT stg_distinct_orders.order_id, 
           stg_distinct_orders.customer_id, 
           payment_amount_per_order.amount_australian_dollar
    FROM payment_amount_per_order
    INNER JOIN stg_distinct_orders
    USING (order_id)
)
SELECT *
FROM final