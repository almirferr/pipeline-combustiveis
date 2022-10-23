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
conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3,org.apache.hadoop:hadoop-common:2.7.3')
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
#conf.set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
#conf.set("com.amazonaws.services.s3.enableV4", "true")

print("versao 4")

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
    
    df = spark.read.csv("s3a://almirbf-teste2/landing/titanic.csv")

    df.show(false)
    df.printSchema()

    print("finishing")
