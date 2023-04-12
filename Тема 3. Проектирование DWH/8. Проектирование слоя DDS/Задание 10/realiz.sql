alter table dds.fct_product_sales 
    add constraint dm_products_product_id_fk foreign key (product_id) references dds.dm_products(id),
    add constraint dm_orders_order_id_fk foreign key (order_id) references dds.dm_orders(id);