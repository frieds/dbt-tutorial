with stg_customers as (
    select * from {{ ref('stg_customers') }}
),
fct_orders AS (
    SELECT *
    FROM {{ ref('fct_orders') }}
),
customer_grouped_details AS (
    select
        stg_orders.customer_id,
        min(fct_orders.order_date) as first_order_date,
        max(fct_orders.order_date) as most_recent_order_date,
        count(fct_orders.order_id) AS count_orders,
        sum(fct_orders.amount_usd_cents) AS total_spend_amount_usd_cents
    FROM fct_orders 
    GROUP BY 1
),
final as (
    select
        stg_customers.customer_id,
        stg_customers.first_name,
        stg_customers.last_name,
        customer_grouped_details.first_order_date,
        customer_grouped_details.most_recent_order_date,
        coalesce(customer_grouped_details.count_orders, 0) as count_orders,
		coalesce(customer_grouped_details.total_spend_amount_usd_cents, 0) AS total_spend_amount_usd_cents
    from stg_customers
    inner join customer_grouped_details 
    using (customer_id)
)
select * from final