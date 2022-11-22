# import libraries
from settings import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from schemas import schemadimcurrency
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType

# main spark program
if __name__ == '__main__':

    # init spark session
    spark = SparkSession \
            .builder \
            .appName("dimcurrency-ysql") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.0.2:8686") \
            .config("spark.hadoop.fs.s3a.access.key", "4jVszc6Opmq7oaOu") \
            .config("spark.hadoop.fs.s3a.secret.key", "ebUjidNSHktNJOhaqeRseqmEr9IEBggD") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.fast.upload", True) \
            .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
            .config("fs.s3a.connection.maximum", 100) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching","false") \
            .getOrCreate()

    # set log level to info
    # [INFO] or [WARN] for more detailed logging info
    spark.sparkContext.setLogLevel("INFO")

    # refer to schemas.py file
    schema = schemadimcurrency
    input_folder = "s3a://lakehouse/gold/example/dimcurrency/"
    destination_table = "public.dimcurrency"


    # read from gold
    gold_table = spark.read.format("delta").load(input_folder)

    gold_table.show()
    gold_table.printSchema()

    # write to yugabyte
    gold_table.write \
    .jdbc(YUGABYTEDB_JDBC, destination_table,
          properties={"user": YUGABYTEDB_USER, "password": YUGABYTEDB_PSWD}).mode("overwrite").load()
          
    #verify count origin vs destination
    #origin_count = gold_table.count()

    #destiny = spark.read \
    #    .format("delta") \
    #    .load(destination_folder)
    
    #destiny_count = destiny.count()

   # print(origin_count)
   # print(destiny_count)

    #if origin_count != destiny_count:
    #    raise AssertionError("Counts of origin and destiny are not equal")

    #spark.stop()