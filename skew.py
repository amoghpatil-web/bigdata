from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
import time

spark =SparkSession.builder.master("local[*]").appName("SaltingPRactical").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df=spark.range(1_000_000).withColumn(
    "key",when(col("id")<900_000,0).otherwise(col("id")%100)

)
print("Orginal Skewed Distribution")
df.groupBy("key").count().orderBy(col("count").desc()).show(3)
print("Optimized GeoupBy using Salting")

salted_df=df.withColumn("salt",col("id")%8)

partial_agg=salted_df.groupBy("key","salt").count()
 
final=partial_agg.groupBy("key").agg(_sum("count").alias("total Count"))

final.orderBy(col("Total Count").desc()).show(3)

print("JobDone open http://localhost:4040 to see the Spark Ui")
time.sleep(60)
spark.stop()
