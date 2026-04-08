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
        .config('spark.mongodb.read.connection.uri', 'mongodb://root:123@mongo:27017') \
        .config('spark.mongodb.write.connection.uri', 'mongodb://root:123@mongo:27017') \
        .getOrCreate()

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

reports_products.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_products') \
    .save()


reports_customers = get_reports_customers(dim_customer, fact_sale)
reports_customers.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_customers') \
    .save()


reports_month_sale = get_reports_month_sale(fact_sale)
reports_month_sale.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_month_sale') \
    .save()

reports_store = get_reports_store(dim_store, fact_sale)
reports_store.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_store') \
    .save()

reports_supplier = get_reports_supplier(dim_supplier, fact_sale)
reports_supplier.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_supplier') \
    .save()



reports_product_reviews = get_reports_product_reviews(dim_product, fact_sale)
reports_product_reviews.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'reports') \
    .option('collection', 'reports_product_reviews') \
    .save()

spark.stop()