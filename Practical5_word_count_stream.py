from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
spark = SparkSession.builder.master("local[2]").appName("LiveWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()
query = word_counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
