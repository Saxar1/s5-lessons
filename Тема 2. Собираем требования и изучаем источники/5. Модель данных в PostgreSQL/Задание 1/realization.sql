select distinct 
	(json_array_elements(event_value::json -> 'product_payments'))::json ->> 'product_name' as product_name
FROM outbox;