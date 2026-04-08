create database if not exists reports;


create table if not exists reports.products (
    product_id UInt32,
	product_name String,
	product_category String,
	rating Decimal(10, 2),
	reviews Int32,
	total_sum Decimal(10, 2),
	total_count Int32
) engine = MergeTree
order by product_id;

create table if not exists reports.customers (
    customer_id UInt32,
	customer_first_name String,
	customer_last_name String,
	customer_country String,
	avg_sum Decimal(10, 2)
) engine = MergeTree
order by customer_id;

create table if not exists reports.month_sale (
    month String,
	avg_sum Decimal(10, 2),
	avg_count Decimal(10, 2)
) engine = MergeTree
order by month;


create table if not exists reports.store (
    store_id UInt32,
	total_sum Decimal(10, 2),
	avg_sum Decimal(10, 2)
) engine = MergeTree
order by store_id;

create table if not exists reports.country_supplier (
	supplier_country String,
	total_sum Decimal(10, 2)
) engine = MergeTree
order by supplier_country;

create table if not exists reports.product_reviews (
	product_id Int32,
	rating Decimal(10, 2),
	reviews UInt32,
	total_quantity Int32
) engine = MergeTree
order by product_id;