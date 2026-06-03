import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from utils.config import (
    BRONZE_PRODUCTS_PATH,
    SILVER_PRODUCTS_PATH
)

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

df = spark.read.options(multiline="true").json(
    os.path.join(BRONZE_PRODUCTS_PATH, "*.json")
)

#explode json array
explode_df = df.select(explode("products").alias("product"))

#flatten the dataframe
flatten_df = explode_df.select(
    "product.id",
    "product.title",
    "product.description",
    "product.price",
    "product.discountPercentage",
    "product.rating",
    "product.stock",
    "product.brand",
    "product.category"
)

#write to silver layer
flatten_df.write.mode("overwrite").parquet(
    SILVER_PRODUCTS_PATH
)
print("Data transformation from Bronze to Silver completed successfully!")