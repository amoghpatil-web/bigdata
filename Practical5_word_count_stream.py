from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

hadoop_bin = os.path.join(os.getcwd(), "win_hadoop", "bin")
os.makedirs(hadoop_bin, exist_ok=True)
for f in ["winutils.exe", "hadoop.dll"]:
    path = os.path.join(hadoop_bin, f)
    if not os.path.exists(path):
        urllib.request.urlretrieve(f"https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/{f}", path)
os.environ["HADOOP_HOME"] = os.path.join(os.getcwd(), "win_hadoop")
os.environ["PATH"] += os.pathsep + hadoop_bin
spark = SparkSession.builder.master("local[2]").appName("LiveWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()
query = word_counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
