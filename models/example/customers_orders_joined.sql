{{ config(
    materialized='table'
) }}

with customers as (
    -- calling the source defined in your .yml file
    select * from {{ source('sales_source', 'raw_customers') }}
),

orders as (
    -- calling the source defined in your .yml file
    select * from {{ source('sales_source', 'raw_orders') }}
),

customer_orders as (

    select
        customers.id as customer_id,
        customers.first_name,
        customers.last_name,
        orders.id as order_id,
        orders.order_date,
        orders.status,
        -- Handle nulls for customers with no orders
        coalesce(orders.amount, 0) as amount

    from customers
    -- Left Join ensures we keep customers even if they haven't ordered yet (like Michael)
    left join orders 
        on customers.id = orders.user_id

)

select * from customer_orders