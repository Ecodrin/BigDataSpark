from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
        .appName('111111') \
        .getOrCreate()

postgres_url = 'jdbc:postgresql://postgres:5432/postgres'
pro = {
	'user': 'postgres',
    'password': '123',
    'driver': 'org.postgresql.Driver'
}

df = spark.read.jdbc(url=postgres_url, table="src.mock_data", properties=pro)


dim_customer = df.select('customer_first_name',
                        'customer_last_name',
                        'customer_age',
                        'customer_email',
                        'customer_country',
                        'customer_postal_code'
                        ).distinct() \
                        .withColumnRenamed('customer_first_name', 'first_name') \
                        .withColumnRenamed('customer_last_name', 'last_name') \
                        .withColumnRenamed('customer_age', 'age') \
                        .withColumnRenamed('customer_email', 'email') \
                        .withColumnRenamed('customer_country', 'country') \
                        .withColumnRenamed('customer_postal_code', 'postal_code') \
                        .withColumn("id", monotonically_increasing_id())

customer_pet = df.select('customer_pet_type',
                    'customer_pet_name',
                    'customer_pet_breed',
                    'customer_email'
                    ).distinct() \
                    .withColumnRenamed('customer_pet_type', 'type') \
                    .withColumnRenamed('customer_pet_name', 'name') \
                    .withColumnRenamed('customer_pet_breed', 'breed')

dim_pet = (
    customer_pet.alias("customer_pet")
    .join(dim_customer.alias('dim_customer'), on=(
            F.col('customer_pet.customer_email') == F.col('dim_customer.email')
        ), how='left')
    ).select(
        F.col('customer_pet.type'),
        F.col('customer_pet.name'),
        F.col('customer_pet.breed'),
        F.col('dim_customer.id').alias('customer_id')
    )

dim_seller = df.select('seller_first_name',
                        'seller_last_name',
                        'seller_email',
                        'seller_country',
                        'seller_postal_code'
                        ).distinct() \
                        .withColumnRenamed('seller_first_name', 'first_name') \
                        .withColumnRenamed('seller_last_name', 'last_name') \
                        .withColumnRenamed('seller_email', 'email') \
                        .withColumnRenamed('seller_country', 'country') \
                        .withColumnRenamed('seller_postal_code', 'postal_code') \
                        .withColumn('id', monotonically_increasing_id())


dim_supplier = df.select('supplier_name',
                        'supplier_contact',
                        'supplier_email',
                        'supplier_phone',
                        'supplier_address',
                        'supplier_city',
                        'supplier_country'
                        ).distinct() \
                        .withColumnRenamed('supplier_name', 'name') \
                        .withColumnRenamed('supplier_contact', 'contact') \
                        .withColumnRenamed('supplier_email', 'email') \
                        .withColumnRenamed('supplier_phone', 'phone') \
                        .withColumnRenamed('supplier_address', 'address') \
                        .withColumnRenamed('supplier_city', 'city') \
                        .withColumnRenamed('supplier_country', 'country') \
                        .withColumn('id', monotonically_increasing_id())

dim_store = df.select(
                        'store_name',
                        'store_location',
                        'store_city',
                        'store_state',
                        'store_country',
                        'store_phone',
                        'store_email',
                    ).distinct() \
                        .withColumnRenamed('store_name', 'name') \
                        .withColumnRenamed('store_location', 'address') \
                        .withColumnRenamed('store_city', 'city') \
                        .withColumnRenamed('store_state', 'state') \
                        .withColumnRenamed('store_country', 'country') \
                        .withColumnRenamed('store_phone', 'phone') \
                        .withColumnRenamed('store_email', 'email') \
                        .withColumn('id', monotonically_increasing_id())


dim_product = df.select('product_name',
                        'product_category',
                        'product_price',
                        'product_quantity',
                        'pet_category',
                        'product_weight',
                        'product_color',
                        'product_size',
                        'product_brand',
                        'product_material',
                        'product_description',
                        'product_rating',
                        'product_reviews',
                        F.to_date('product_release_date', 'M/d/yyyy').alias('release_date'),
                        F.to_date('product_expiry_date', 'M/d/yyyy').alias('expiry_date')
                        ).distinct() \
                        .withColumnRenamed('product_name', 'name') \
                        .withColumnRenamed('product_category', 'category') \
                        .withColumnRenamed('product_price', 'price') \
                        .withColumnRenamed('product_quantity', 'quantity') \
                        .withColumnRenamed('product_weight', 'weight') \
                        .withColumnRenamed('product_color', 'color') \
                        .withColumnRenamed('product_size', 'size') \
                        .withColumnRenamed('product_description', 'description') \
                        .withColumnRenamed('product_rating', 'rating') \
                        .withColumnRenamed('product_reviews', 'reviews') \
                        .withColumn('id', monotonically_increasing_id())

fact_sale = (
        df.alias('df').join(
            dim_customer.alias('dim_customer'),
            on=(
                F.col('df.customer_email') == F.col('dim_customer.email')
            ),
            how='left'
        ).join(
            dim_seller.alias('dim_seller'),
            on=(
                F.col('df.seller_email') == F.col('dim_seller.email')
            ),
            how='left'
        ).join(
            dim_product.alias('dim_product'),
            on=(
                (F.col('df.product_name') == F.col('dim_product.name')) &
                (F.col('df.product_price') == F.col('dim_product.price')) &
                (F.col('df.product_weight') == F.col('dim_product.weight')) &
                (F.col('df.product_brand') == F.col('dim_product.product_brand')) &
                (F.to_date(F.col('df.product_release_date'), 'M/d/yyyy') == F.col('dim_product.release_date')) &
                (F.to_date(F.col('df.product_expiry_date'), 'M/d/yyyy') == F.col('dim_product.expiry_date'))
            ),
            how='left'
        ).join(
            dim_supplier.alias('dim_supplier'),
            on=(
                F.col('df.supplier_email') == F.col('dim_supplier.email')
            ),
            how='left'
        ).join(
            dim_store.alias('dim_store'),
            on=(
                F.col('df.store_email') == F.col('dim_store.email')
            ),
            how='left'
        )
    ).select(
        F.to_date('df.sale_date', 'M/d/yyyy').alias('sale_date'),
        F.col('dim_customer.id').alias('customer_id'),
        F.col('dim_seller.id').alias('seller_id'),
        F.col('dim_product.id').alias('product_id'),
        F.col('dim_store.id').alias('store_id'),
        F.col('dim_supplier.id').alias('supplier_id'),
        F.col('df.sale_quantity').alias('quantity'),
        F.col('df.sale_total_price').alias('total_price')
    ).withColumn('id', monotonically_increasing_id())



dim_customer.write.jdbc(
    url=postgres_url,
    table='dwh.dim_customer',
    mode='overwrite',
    properties=pro
)


dim_pet.write.jdbc(
    url=postgres_url,
    table='dwh.dim_pet',
    mode='overwrite',
    properties=pro
)

dim_seller.write.jdbc(
    url=postgres_url,
    table='dwh.dim_seller',
    mode='overwrite',
    properties=pro
)

dim_supplier.write.jdbc(
    url=postgres_url,
    table='dwh.dim_supplier',
    mode='overwrite',
    properties=pro
)

dim_store.write.jdbc(
    url=postgres_url,
    table='dwh.dim_store',
    mode='overwrite',
    properties=pro
)

dim_product.write.jdbc(
    url=postgres_url,
    table='dwh.dim_product',
    mode='overwrite',
    properties=pro
)

fact_sale.write.jdbc(
    url=postgres_url,
    table='dwh.fact_sale',
    mode='overwrite',
    properties=pro
)


spark.stop()