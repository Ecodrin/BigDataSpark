from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, sum, monotonically_increasing_id


def get_reports_sale(dim_product, fact_sale):
    return (
        fact_sale.alias('fact_sale').join(
            dim_product.alias('dim_product'),
            on=(
                F.col('fact_sale.product_id') == F.col('dim_product.id')
            ),
            how='left'
        )
    ).select(
        F.col('dim_product.id').alias('product_id'),
        F.col('dim_product.name').alias('product_name'),
        F.col('dim_product.category').alias('product_category'),
        F.col('dim_product.rating').alias('rating'),
        F.col('dim_product.reviews').alias('reviews'),
        F.col('fact_sale.total_price').alias('total_price'),
        F.col('fact_sale.quantity').alias('quantity')
    ).groupBy('product_id', 'product_name', 'product_category', 'rating', 'reviews') \
        .agg(
            sum('total_price').alias('total_sum'),
            sum('quantity').alias('total_quantity')
            ) \
    .orderBy('total_sum', ascending=False)


def get_reports_customers(dim_customer, fact_sale):
    return (
        fact_sale.alias('fact_sale').join(
            dim_customer.alias('dim_customer'),
            on=(
                F.col('fact_sale.customer_id') == F.col('dim_customer.id')
            ),
            how='left'
        )
    ).select(
        F.col('dim_customer.id').alias('customer_id'),
        F.col('dim_customer.first_name').alias('customer_first_name'),
        F.col('dim_customer.last_name').alias('customer_last_name'),
        F.col('dim_customer.country').alias('customer_country'),
        F.col('fact_sale.total_price').alias('total_price'),
    ).groupBy('customer_id', 'customer_first_name', 'customer_last_name', 'customer_country') \
        .agg(
            avg('total_price').alias('avg_sum')
            ) \
    .orderBy('customer_id', ascending=True)

def get_reports_month_sale(fact_sale):
    return fact_sale.alias('fact_sale').select(
        F.date_format('fact_sale.sale_date', 'MMMM').alias('month'),
        F.col('fact_sale.total_price').alias('total_price'),
        F.col('fact_sale.quantity').alias('quantity')
        ).groupBy('month').agg(
            avg('total_price').alias('avg_sum'),
            avg('quantity').alias('avg_count'),
            )


def get_reports_store(dim_store, fact_sale):
    return (
        fact_sale.alias('fact_sale').join(
            dim_store.alias('dim_store'),
            on=(
                F.col('fact_sale.store_id') == F.col('dim_store.id')
            ),
            how='left'
        )
    ).select(
        F.col('dim_store.id').alias('store_id'),
        F.col('fact_sale.total_price').alias('total_price'),
        F.col('dim_store.country').alias('store_country'),
        F.col('dim_store.city').alias('store_city'),
        F.col('dim_store.state').alias('store_state'),
        F.col('dim_store.address').alias('store_address')
    ).groupBy('store_id', 'store_country', 'store_city', 'store_state', 'store_address') \
        .agg(
            sum('total_price').alias('total_sum'),
            avg('total_price').alias('avg_sum')
            ) \
    .orderBy('total_sum', ascending=False)

def get_reports_supplier(dim_supplier, fact_sale):
    return (
        fact_sale.alias('fact_sale').join(
            dim_supplier.alias('dim_supplier'),
            on=(
                F.col('fact_sale.supplier_id') == F.col('dim_supplier.id')
            ),
            how='left'
        )
    ).select(
        F.col('fact_sale.total_price').alias('total_price'),
        F.col('dim_supplier.country').alias('supplier_country')
    ).groupBy('supplier_country') \
        .agg(
            sum('total_price').alias('total_sum')
            ) \
    .orderBy('total_sum', ascending=False)


def get_reports_product_reviews(dim_product, fact_sale):
    return (
        fact_sale.alias('fact_sale').join(
            dim_product.alias('dim_product'),
            on=(
                F.col('fact_sale.product_id') == F.col('dim_product.id')
            ),
            how='left'
        )
    ).select(
        F.col('dim_product.id').alias('product_id'),
        F.col('dim_product.rating').alias('rating'),
        F.col('dim_product.reviews').alias('reviews'),
        F.col('fact_sale.total_price').alias('total_price'),
        F.col('fact_sale.quantity').alias('quantity')
    ).groupBy('product_id','rating', 'reviews') \
        .agg(
            sum('quantity').alias('total_quantity')
        ) \
    .orderBy('reviews', ascending=False)