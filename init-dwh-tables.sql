CREATE DATABASE books;

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

--
-- bronze
--
create or replace table books.bronze.entities_current_load(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

create or replace table books.bronze.entities_previous_load(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

create or replace table books.bronze.reads_current_load(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

create or replace table books.bronze.reads_previous_load(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

create or replace table books.bronze.entities_dedup(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

create or replace table books.bronze.reads_dedup(
    id number not null autoincrement primary key,
    name varchar not null unique,
    created_at datetime not null default current_timestamp,
    source varchar not null,
    md5 varchar not null
);

--
-- silver
--




--
-- gold
--