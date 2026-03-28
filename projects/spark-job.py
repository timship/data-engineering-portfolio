import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, percent_rank
from pyspark.sql.window import Window

DATA_PATH = f"s3a://startde-raw/raw_items"
TARGET_PATH = f"s3a://startde-project/marija-shkurat-wrn7887/seller_items"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def main():
    spark = _spark_session()
    orders_df = spark.read.parquet(DATA_PATH)
    window_spec = Window.orderBy("item_rate")
    orders_df = (
        orders_df
        .withColumn(
            "returned_items_count",
            (col("ordered_items_count") * col("avg_percent_to_sold")).cast("int")
        )
        .withColumn(
            "potential_revenue",
            (
                    (col("availability_items_count") + col("ordered_items_count"))
                    * col("item_price")
            ).cast("bigint")
        )
        .withColumn(
            "total_revenue",
            (
                    (col("goods_sold_count") - col("returned_items_count"))
                    * col("item_price")
            ).cast("bigint")
        )
        .withColumn(
            "avg_daily_sales",
            when(col("days_on_sell") > 0,
                 col("goods_sold_count") / col("days_on_sell"))
            .otherwise(0.0)
            .cast("double")
        )
        .withColumn(
            "days_to_sold",
            when(col("avg_daily_sales") > 0,
                 col("availability_items_count") / col("avg_daily_sales"))
            .otherwise(None)
            .cast("double")
        )
        .withColumn(
            "item_rate_percent",
            percent_rank().over(window_spec)
        )
    )
    orders_df.printSchema()
    orders_df.show()
    orders_df.write.mode("overwrite").parquet(TARGET_PATH)
    # ОБЯЗАТЕЛЬНО ДОБАВИТЬ ИНАЧЕ ПОД ОСТАНЕТСЯ ВИСЕТЬ
    spark.stop()


if __name__ == "__main__":
    main()
