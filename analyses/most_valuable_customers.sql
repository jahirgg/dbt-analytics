with fct_orders as (
    select *
    from {{ ref('fct_orders')}}
),
dim_customers as (
    select *
    from {{ ref('dim_customers')}}
),
analysis as (
    SELECT
        cust.customer_id,
        cust.first_name,
        SUM(total_amount) AS global_paid_amount
    FROM fct_orders AS ord
    LEFT JOIN dim_customers AS cust ON ord.customer_id = cust.customer_id
    WHERE ord.is_order_completed = 1
    GROUP BY cust.customer_id, first_name
    ORDER BY 3 DESC
    LIMIT 10
)

select *
from analysis
