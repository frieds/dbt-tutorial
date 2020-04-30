{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
		select * from {{ ref('stg_payments') }}
),
customer_order_aggregates as (
    select
        orders.customer_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders,
		SUM(payments.amount_australian_dollar) as total_order_amount_australian_dollar
    from orders
	LEFT JOIN payments
	ON payments.order_id = orders.order_id
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_order_aggregates.first_order_date,
        customer_order_aggregates.most_recent_order_date,
        coalesce(customer_order_aggregates.number_of_orders, 0) as number_of_orders,
		customer_order_aggregates.total_order_amount_australian_dollar
    from customers
    inner join customer_order_aggregates using (customer_id)
)
select * from final