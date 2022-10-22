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

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

# apply config
sc = SparkContext(conf=conf).getOrCreate()

if __name__ == "__main__":
    
    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()
    
    print("setting")
    
    spark.sparkContext.setLogLevel("WARN")

    print("reading")
    
    df = spark.read.csv("s3a://dl-landing-zone-539445819059/titanic/titanic.csv")
    df.show(false)
    df.printSchema()

    print("finishing")
