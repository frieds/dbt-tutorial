with source_payments as (
    -- schema is stripe, table is payment
    select * from {{ source('stripe', 'payment') }}

), staging_payments AS (
select
    ID AS payment_id,
    "orderID" AS order_id,
    "paymentMethod" AS payment_method,
    amount/100 AS amount_usd_cents,
    created AS processed_date
from source_payments
) 
select * from staging_payments