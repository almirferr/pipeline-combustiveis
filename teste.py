from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

print("*****************")
print("Iniciando!!!")
print("*****************")

aws_access_key_id = "AKIAYBEEQYFSDKNFEHG7"
aws_secret_access_key = "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"

print("********aws_access_key_id*********")
print(aws_access_key_id)
print("*****************")

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()
    
    print("setting")
    
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    spark.sparkContext.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    spark.sparkContext.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

    print("reading")
    
    df = spark.read.csv("s3a://dl-landing-zone-539445819059/titanic/titanic.csv")
    df.show(false)
    df.printSchema()

    print("finishing")
