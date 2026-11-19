//pip install pyspark graphframes

import os
import sys
import urllib.request
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


hadoop_home = os.path.join(os.getcwd(), "hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.makedirs(hadoop_bin, exist_ok=True)
winutils_path = os.path.join(hadoop_bin, "winutils.exe")

if not os.path.exists(winutils_path):
    url = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.0.0/bin/winutils.exe"
    urllib.request.urlretrieve(url, winutils_path)

os.environ["HADOOP_HOME"] = hadoop_home

spark = SparkSession.builder.appName("Pure_PySpark_PageRank").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


vertices = spark.createDataFrame([
    (1, "Alice"), (2, "Bob"), (3, "Charlie"),
    (4, "David"), (5, "Emma"), (6, "Frank")
], ["id", "name"])


edges = spark.createDataFrame([
    (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
    (1, 2), (2, 3), (3, 4)
], ["src", "dst"])

print("\n✅ Graph Created Successfully")
print(f"Total Users: {vertices.count()}")
print(f"Total Relationships: {edges.count()}\n")
print("Running PageRank natively...\n")


outDegrees = edges.groupBy("src").count().withColumnRenamed("count", "outDegree")


ranks = vertices.select("id").withColumn("rank", F.lit(1.0))


for _ in range(10):
    contribs = edges.join(ranks, edges.src == ranks.id) \
                    .join(outDegrees, edges.src == outDegrees.src) \
                    .withColumn("contrib", F.col("rank") / F.col("outDegree")) \
                    .select("dst", "contrib")
    
    sum_contribs = contribs.groupBy("dst").agg(F.sum("contrib").alias("sum_contribs"))
    
    ranks = vertices.select("id") \
                    .join(sum_contribs, vertices.id == sum_contribs.dst, "left_outer") \
                    .fillna(0.0, subset=["sum_contribs"]) \
                    .withColumn("rank", 0.15 + 0.85 * F.col("sum_contribs")) \
                    .select("id", "rank")


print("📊 PageRank Results:")
results = vertices.join(ranks, "id").select("name", F.round("rank", 4).alias("pagerank")).orderBy(F.desc("pagerank"))
results.show()


top_user = results.first()
print("🏆 Most Influential User:")
print(f"{top_user['name']} with a score of {top_user['pagerank']}\n")
