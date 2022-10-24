from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("criando contexto!")

#    conf = spark.sparkContext._jsc.hadoopConfiguration()
#    conf.set("fs.gs.auth.service.account.enable", "true")
#    conf.set("fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json")
#    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")    

    print("*****************")
    print("Iniciando!!!")
    print("*****************")

    print("hadoopConfiguration!")
#    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","c:\gcp-secret-keys.json")

    print("read csv gs!")
#    df=spark.read.csv(f"gs://landing-zone-202210182226/titanic.csv", header=True, sep=";")
#    df.show()

    print("write gs!")
#    df.write.mode("overwrite").format("parquet").save(f"gs://silver-zone-202210182226/parquet/") 

    print("read parquet gs!")
#    df2=spark.read.parquet(f"gs://silver-zone-202210182226/parquet")

#    df2.show()

    print("*****************")
    print("Escrito com sucesso!")
    print("*****************")

    spark.stop()
