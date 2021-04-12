WITH stg_payments AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
),
final AS (
    SELECT stg_payments.order_id, 
           SUM(stg_payments.amount_australian_dollar) AS amount_australian_dollar
    FROM stg_payments
    GROUP BY 1 
)
SELECT *
FROM final