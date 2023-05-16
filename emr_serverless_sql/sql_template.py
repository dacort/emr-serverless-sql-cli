from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.enableHiveSupport()
    .appName("Python Spark SQL basic example")
    .getOrCreate()
)

df = spark.sql("$query")
df.show()
