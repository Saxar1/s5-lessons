create table dds.dm_users (
    id serial not null primary key,
    user_id varchar not null,
    user_name varchar not null,
    user_login varchar not null
)