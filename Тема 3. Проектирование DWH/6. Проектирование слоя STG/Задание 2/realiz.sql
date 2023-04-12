CREATE TABLE stg.ordersystem_users (
    id serial CONSTRAINT ordersystem_users_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_users_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);
CREATE TABLE stg.ordersystem_orders (
    id serial CONSTRAINT ordersystem_orders_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);
CREATE TABLE stg.ordersystem_restaurants (
    id serial CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY,
    object_id varchar NOT NULL CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);
