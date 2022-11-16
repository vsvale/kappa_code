# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, current_date, col, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("example-currency-silver-py") \
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
    origin_folder = "s3a://lakehouse/silver/example/dimcurrency/"
    destination_folder = "s3a://lakehouse/gold/example/dimcurrency/"
    write_delta_mode = "overwrite"

    DeltaTable.createIfNotExists(spark) \
        .tableName("dimcurrency") \
        .addColumn("CurrencyKey", IntegerType()) \
        .addColumn("CurrencyAlternateKey", StringType()) \
        .addColumn("CurrencyName", StringType()) \
        .addColumn("create_at", TimestampType()) \
        .addColumn("load_date", DateType(), generatedAlwaysAs="CAST(create_at AS DATE)") \
        .partitionedBy("load_date") \
        .location(destination_folder) \
        .execute()
    
    # read data

    origin_table = spark.read.format("delta").load(origin_folder)
    gold_table = gold_table.withColumn("create_at", current_timestamp())
    gold_table = gold_table.alias("c") 
   
    if DeltaTable.isDeltaTable(spark, destination_folder):
        dt_table = DeltaTable.forPath(spark, destination_folder)
        dt_table.alias("historical_data")\
            .merge(
                gold_table.alias("new_data"),
                '''
                historical_data.CurrencyKey = new_data.CurrencyKey 
                ''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        gold_table.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("load_date")\
            .save(destination_folder)



    gold_table.write \
    .jdbc("jdbc:postgresql://yb-tservers.database.svc.Cluster.local:5433/salesdw", "public.dimcurrency",
          properties={"user": "plumber", "password": "PlumberSDE"}).mode("overwrite").load()
          

    # stop session
    spark.stop()