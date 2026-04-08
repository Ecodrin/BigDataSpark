from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sum, monotonically_increasing_id

from reports import get_reports_sale, \
                get_reports_customers, \
                get_reports_month_sale, \
                get_reports_store,       \
                get_reports_supplier,     \
                get_reports_product_reviews

spark = SparkSession.builder \
        .appName('22222') \
        .getOrCreate()

clickhouse_url = f'jdbc:clickhouse://clickhouse:8123/default'
clickhouse_pro = {
    'user': 'default',
    'password': '123',
    'driver': 'com.clickhouse.jdbc.ClickHouseDriver'
}

postgres_url = 'jdbc:postgresql://postgres:5432/postgres'
postgres_pro = {
	'user': 'postgres',
    'password': '123',
    'driver': 'org.postgresql.Driver'
}

dim_product = spark.read.jdbc(
    url=postgres_url,
    table='dwh.dim_product',
    properties=postgres_pro
)

dim_customer = spark.read.jdbc(
    url=postgres_url,
    table='dwh.dim_customer',
    properties=postgres_pro
)

dim_store = spark.read.jdbc(
    url=postgres_url,
    table='dwh.dim_store',
    properties=postgres_pro
)

dim_supplier = spark.read.jdbc(
    url=postgres_url,
    table='dwh.dim_supplier',
    properties=postgres_pro
)

fact_sale = spark.read.jdbc(
    url=postgres_url,
    table='dwh.fact_sale',
    properties=postgres_pro
)

reports_products = get_reports_sale(dim_product, fact_sale)


reports_products.write.jdbc(
    url=clickhouse_url,
    table='reports.products',
    mode='overwrite',
    properties=clickhouse_pro
)


reports_customers = get_reports_customers(dim_customer, fact_sale)
reports_customers.write.jdbc(
    url=clickhouse_url,
    table='reports.customers',
    mode='overwrite',
    properties=clickhouse_pro
)


reports_month_sale = get_reports_month_sale(fact_sale)
reports_month_sale.write.jdbc(
    url=clickhouse_url,
    table='reports.month_sale',
    mode='overwrite',
    properties=clickhouse_pro
)

reports_store = get_reports_store(dim_store, fact_sale)
reports_store.write.jdbc(
    url=clickhouse_url,
    table='reports.store',
    mode='overwrite',
    properties=clickhouse_pro
)

reports_supplier = get_reports_supplier(dim_supplier, fact_sale)
reports_supplier.write.jdbc(
    url=clickhouse_url,
    table='reports.country_supplier',
    mode='overwrite',
    properties=clickhouse_pro
)



reports_product_reviews = get_reports_product_reviews(dim_product, fact_sale)
reports_product_reviews.write.jdbc(
    url=clickhouse_url,
    table='reports.product_reviews',
    mode='overwrite',
    properties=clickhouse_pro
)

spark.stop()