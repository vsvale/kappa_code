# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, current_date, col, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("example-promotion-bronze-py") \
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
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # variables
    origin_folder = "s3a://landing/example/dw-files/DimPromotion.csv"
    destination_folder = "s3a://lakehouse/bronze/example/dimpromotion/"
    write_delta_mode = "overwrite"
    # read data

    schema = StructType([
    StructField("PromotionKey", IntegerType(), True),
    StructField("PromotionAlternateKey", IntegerType(), True),
    StructField("EnglishPromotionName", StringType(), True),
    StructField("SpanishPromotionName", StringType(), True),
    StructField("FrenchPromotionName", StringType(), True),
    StructField("DiscountPct", DoubleType(), True),
    StructField("EnglishPromotionType", StringType(), True),
    StructField("SpanishPromotionType", StringType(), True),
    StructField("FrenchPromotionType", StringType(), True),
    StructField("EnglishPromotionCategory", StringType(), True),
    StructField("SpanishPromotionCategory", StringType(), True),
    StructField("FrenchPromotionCategory", StringType(), True),
    StructField("StartDate", DateType(), True),
    StructField("EndDate", DateType(), True),
    StructField("MinQty", IntegerType(), True),
    StructField("MaxQty", IntegerType(), True)])

    bronze_table = spark.read.options(header='False',delimiter='|').csv(origin_folder, schema=schema)
   
    if DeltaTable.isDeltaTable(spark, destination_folder):
        dt_table = DeltaTable.forPath(spark, destination_folder)
        dt_table.alias("historical_data")\
            .merge(
                bronze_table.alias("new_data"),
                '''
                historical_data.PromotionKey = new_data.PromotionKey 
                ''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        bronze_table.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("b_load_date")\
            .save(destination_folder)

    #verify count origin vs destination
    origin_count = bronze_table.count()

    destiny = spark.read \
        .format("delta") \
        .load(destination_folder)
    
    destiny_count = destiny.count()

    if origin_count != destiny_count:
        raise AssertionError("Counts of origin and destiny are not equal")

    # stop session
    spark.stop()