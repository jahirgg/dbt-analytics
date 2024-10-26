with order_payments as (
   select * from {{ ref('stg_stripe_order_payments') }}
   where status = 'success'
)

select
    order_id,
    sum(
        case
            when payment_type = 'cash' 
            then amount
            else 0
        end
        ) as cash_amount,
    sum(
        case
            when payment_type = 'credit'
            then amount
            else 0
        end
        ) as credit_amount,
    sum(amount
        ) as total_amount
from order_payments
group by 1