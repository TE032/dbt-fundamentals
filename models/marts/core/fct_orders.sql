with orders as(
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payment as(
    select * from {{ ref('stg_payments') }}
),
final as(
    select 
        o.ORDER_ID, 
        c.CUSTOMER_ID, 
        sum(p.amount)/100 as amount 
from orders o join payment p on o.ORDER_ID = p.ORDERID
join customers c on c.CUSTOMER_ID = o.CUSTOMER_ID
and p.status='success'
group by o.ORDER_ID,c.CUSTOMER_ID
)
select * from final

