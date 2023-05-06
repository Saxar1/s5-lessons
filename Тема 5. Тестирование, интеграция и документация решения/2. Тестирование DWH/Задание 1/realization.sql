select
	current_timestamp as test_date_time,
	'test_01' as test_name,
	False as test_result
from public_test.dm_settlement_report_expected dsre
full join public_test.dm_settlement_report_actual dsra using(restaurant_name)
where dsra.id is null or dsre.id is null
limit 1;