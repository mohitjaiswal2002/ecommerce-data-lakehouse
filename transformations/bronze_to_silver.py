from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

df = spark.read.options(multiline="true") \
          .json("ecommerce-data-lakehouse/data/bronze/products/*.json")

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
flatten_df.write.mode("overwrite").parquet("ecommerce-data-lakehouse/data/silver/products")
print("Data transformation from Bronze to Silver completed successfully!")