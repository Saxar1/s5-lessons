with 
metrics as (
	select 
		dr.id as restaurant_id,
		dr.restaurant_name as restaurant_name,
		dt."date" as settlement_date,
		do2.id as order_id,
		fps.total_sum as total_sum,
		fps.bonus_payment as bonus_payment,
		fps.bonus_grant as bonus_grant
	from dds.dm_restaurants dr
	join dds.dm_orders do2 on dr.id = do2.restaurant_id
	join dds.dm_timestamps dt on do2.timestamp_id = dt.id 
	join dds.fct_product_sales fps on do2.id = fps.order_id
	where do2.order_status = 'CLOSED')
insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum,
orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
select
	m.restaurant_id,
	m.restaurant_name,
	m.settlement_date,
	count(distinct m.order_id) as orders_count,
	sum(m.total_sum) as orders_total_sum,
	sum(m.bonus_payment) as orders_bonus_payment_sum,
	sum(m.bonus_grant) as orders_bonus_granted_sum,
	sum(m.total_sum) * 0.25 as order_processing_fee,
	(sum(m.total_sum) - sum(m.total_sum) * 0.25 - sum(m.bonus_payment)) as restaurant_reward_sum
from metrics as m
group by m.restaurant_id, m.restaurant_name, settlement_date;


