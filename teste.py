from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

if __name__ == "__main__":
    spark = SparkSession.builder()
          .master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key_id)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    df = spark.read.csv("s3a://dl-landing-zone-539445819059/titanic/titanic.csv")
    df.show(false)
    df.printSchema()
