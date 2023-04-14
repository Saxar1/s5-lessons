-- restaurant_reward_sum
-- restaurant_id
-- restaurant_name
-- settlement_date
-- orders_count
-- orders_total_sum
-- orders_bonus_payment_sum
-- orders_bonus_granted_sum
-- order_processing_fee

with 
restaurant_m as (
    select *
    from dds.dm_restaurants),
order_m as (
    select
        id,
    	restaurant_id,
    	timestamp_id
    from dds.dm_orders
    where order_status = 'CLOSED'),
date_m as (
	select
		id,
		"date"
	from dds.dm_timestamps),
fps_m as (
    select
        order_id,
        total_sum,
        bonus_payment,
        bonus_grant
    from dds.fct_product_sales)
select 
	rm.id,
	rm.restaurant_name,
	dm.date as settlement_date,
	count(om.id) as orders_count,
	sum(fm.total_sum) as order_total_sum,
	sum(fm.bonus_payment) as order_bonus_payment_sum,
	sum(fm.bonus_grant) as order_bonus_granted_sum,
	sum(fm.total_sum) * 0.25 as order_processing_fee,
	(sum(fm.total_sum) - sum(fm.total_sum) * 0.25 - sum(fm.bonus_payment)) as restaurant_reward_sum
from restaurant_m as rm
join order_m as om on om.restaurant_id = rm.id
join date_m as dm on dm.id = om.timestamp_id
join fps_m as fm on om.id = fm.order_id
group by rm.id, rm.restaurant_name, dm.date;







