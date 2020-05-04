with source_customers as (
    -- schema jaffle_shop; table customers
    select * from {{ source('jaffle_shop', 'customers') }}
),
stg_customers as (
    select
        id as customer_id,
        first_name,
        last_name
    from source_customers
)
select * from stg_customers