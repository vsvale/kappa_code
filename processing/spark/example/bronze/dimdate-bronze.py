# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, current_date, col, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("example-date-bronze-py") \
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
    origin_folder = "s3a://landing/example/dw-files/DimDate.csv"
    destination_folder = "s3a://lakehouse/bronze/example/dimdate/"
    write_delta_mode = "overwrite"
    # read data

    schema = StructType([
    StructField("DateKey", IntegerType(), True),
    StructField("FullDateAlternateKey", DateType(), True),
    StructField("DayNumberOfWeek", IntegerType(), True),
    StructField("EnglishDayNameOfWeek", StringType(), True),
    StructField("SpanishDayNameOfWeek", StringType(), True),
    StructField("FrenchDayNameOfWeek", StringType(), True),
    StructField("DayNumberOfMonth", IntegerType(), True),
    StructField("DayNumberOfYear", IntegerType(), True),
    StructField("WeekNumberOfYear", IntegerType(), True),
    StructField("EnglishMonthName", StringType(), True),
    StructField("SpanishMonthName", StringType(), True),
    StructField("FrenchMonthName", StringType(), True),
    StructField("MonthNumberOfYear", IntegerType(), True),
    StructField("CalendarQuarter", IntegerType(), True),
    StructField("CalendarYear", IntegerType(), True),
    StructField("CalendarSemester", IntegerType(), True),
    StructField("FiscalQuarter", IntegerType(), True),
    StructField("FiscalYear", IntegerType(), True),
    StructField("FiscalSemester", IntegerType(), True)])

    landing_table = spark.read.options(header='False',delimiter='|').csv(origin_folder, schema=schema)
    landing_table = landing_table.withColumn("b_create_at", current_timestamp())
    landing_table = landing_table.withColumn("b_load_date", current_date())
   
    if DeltaTable.isDeltaTable(spark, destination_folder):
        dt_table = DeltaTable.forPath(spark, destination_folder)
        dt_table.alias("historical_data")\
            .merge(
                landing_table.alias("new_data"),
                '''
                historical_data.DateKey = new_data.DateKey 
                ''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        landing_table.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("b_load_date")\
            .save(destination_folder)

    #verify count origin vs destination
    origin_count = landing_table.count()

    destiny = spark.read \
        .format("delta") \
        .load(destination_folder)
    
    destiny_count = destiny.count()

    if origin_count != destiny_count:
        raise AssertionError("Counts of origin and destiny are not equal")

    # stop session
    spark.stop()