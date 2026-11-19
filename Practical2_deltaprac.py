
//pip install pyspark delta-spark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import shutil

builders = SparkSession.builder.appName("DeltaPractical") \
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builders).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

delta_path="./my_delta_table"
shutil.rmtree(delta_path, ignore_errors=True)

print("Initial Data(Version 0)")
data =[(1,"Amogh",5000),(2,"Saniya",6000),(3,"Aditya",6000)]
df=spark.createDataFrame(data,["id","name","salary"])
df.write.format("delta").mode("overwrite").save(delta_path)
spark.read.format("delta").load(delta_path).show()

deltaTable = DeltaTable.forPath(spark, delta_path)

print("ACID Update Saniya gets +1000 raise(Version1)")
deltaTable.update(condition="name='Saniya'", set={"salary":"salary+1000"})
spark.read.format("delta").load(delta_path).show()

print("ACID Delete Aditya leaves the company(Version2)")
deltaTable.delete(condition="name='Aditya'")
spark.read.format("delta").load(delta_path).show()

print("Reding orginal data")
df_version=spark.read.format("delta").option("versionAsOf",0).load(delta_path)
df_version.show()
spark.stop()
