with stg_customers as (
    select * from {{ ref('stg_customers') }}
),
stg_orders as (
    select * from {{ ref('stg_orders') }}
),
payment_amount_per_order AS (
    SELECT *
    FROM {{ ref('payment_amount_per_order') }}
),
stg_distinct_orders AS (
    SELECT DISTINCT customer_id, order_id
    FROM {{ ref('stg_orders') }}
),
customer_order_amounts AS (
    SELECT payment_amount_per_order.order_id, 
           stg_distinct_orders.customer_id, 
           payment_amount_per_order.amount_australian_dollar
    FROM payment_amount_per_order
    INNER JOIN stg_distinct_orders
    USING (order_id)
),
customer_grouped_details AS (
    select
        customer_id,
        min(stg_orders.order_date) as first_order_date,
        max(stg_orders.order_date) as most_recent_order_date,
        count(customer_order_amounts.order_id) AS count_orders,
        sum(customer_order_amounts.amount_australian_dollar) AS amount_australian_dollar
    FROM customer_order_amounts
    INNER JOIN stg_orders 
    USING (customer_id)
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
		customer_grouped_details.amount_australian_dollar
    from stg_customers
    inner join customer_grouped_details 
    using (customer_id)
)
select * from final