import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

print("Criando session!")

spark = (
    SparkSession
    .builder
    .appName("combustiveis_app")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("criando contexto!")

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.auth.service.account.enable", "true")
conf.set("fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json")
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

#-------------------------------
print("buscando titanic!")

df = (
    spark
    .read
    .format("csv")
    .options(header='true', inferSchema='true', delimiter=';')
    .load("gs://landing-zone-202210182226/titanic.csv")
)

print("gravando titanic!")
(
    df
    .write
    .format("parquet")
    .mode("overwrite")
    .save("gs://silver-zone-202210182226/titanic")
)

#-------------------------------

print("lendo combustiveis!")

df_landing = (
    spark
    .read
    .format("csv")
    .options(header='true', inferSchema='true', delimiter=';')
    .load("gs://landing-zone-202210182226/combustiveis/ca-2008-01.csv")
)

df_landing.printSchema()

import sys
import pyspark.sql.functions as f
import re

print("normalizando combustiveis!")

def normalize_column(column_name: str):
  column_normalized = re.sub("[-_]", " ", column_name.strip()) 
  column_normalized = column_normalized.lower()
  return re.sub(" +", "_", column_normalized)

for column in df_landing.columns:
    df_landing = df_landing.withColumnRenamed(column, normalize_column(column))

df_landing.printSchema()

print("trocando , combustiveis!")

df_landing = (
      df_landing
      .withColumn(
        "valor_de_venda", 
        f.regexp_replace("valor_de_venda", ',', '.')
        .cast('double'))
      .withColumn(
        "valor_de_compra", 
        f.regexp_replace("valor_de_compra", ',', '.')
        .cast('double'))
    )

print("salvando combustiveis!")

(
    df_landing
    .write
    .format("parquet")
    .mode("overwrite")
    .save("gs://silver-zone-202210182226/combustiveis")
)

print("finalizado!")