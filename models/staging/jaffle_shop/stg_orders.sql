with source_customers as (
    -- schema jaffle_shop; table order
    select * from {{ source('jaffle_shop', 'orders') }}
),
stg_customers as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from source_customers
)
select * from stg_customers