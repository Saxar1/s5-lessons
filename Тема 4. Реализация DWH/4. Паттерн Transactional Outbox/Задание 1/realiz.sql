CREATE TABLE public.outbox (
    id int                not null,
    object_id int not null,
    record_ts timestamp not null,
    "type" varchar not null,
    payload text not null 
);