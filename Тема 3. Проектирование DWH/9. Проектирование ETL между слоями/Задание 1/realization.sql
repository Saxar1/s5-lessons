-- restaurant_reward_sum
-- restaurant_id
-- restaurant_name
-- settlement_date
-- orders_count
-- orders_total_sum
-- orders_bonus_payment_sum
-- orders_bonus_granted_sum
-- order_processing_fee


select
    fps.count as orders_count,
    fps.total_sum as orders_total_sum,
    fps.bonus_payment as orders_bonus_payment_sum,
    fps.bonus_granted as orders_bonus_granted_sum,
    order_processing_fee
from dds.fct_product_sales as fps`