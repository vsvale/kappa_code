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
            .appName("dimcurrency-gold") \
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
    input_topic = "dimcurrency_spark_stream_dwfiles"
    destination_folder = "s3a://lakehouse/gold/example/dimcurrency/"
    write_delta_mode = "overwrite"
    destination_table = "public.dimcurrency"
    destination_topic = "gold_dimcurrency"
    jsonOptions = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"}

    # reading data from apache kafka
    # stream operation mode
    # latest offset recorded on kafka and spark
    stream_table= spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .option("checkpoint", "checkpoint") \
        .load() \
        .select(from_json(col("value").cast("string"), schema, jsonOptions))

# batch
#    stream_table= spark \
#        .read \
#        .format("kafka") \
#        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#        .option("subscribe", input_topic) \
#        .option("startingOffsets", "earliest") \
#        .option("checkpoint", "checkpoint") \
#        .load()

    # write to gold
    DeltaTable.createIfNotExists(spark) \
        .tableName("dimcurrency") \
        .addColumn("CurrencyKey", IntegerType()) \
        .addColumn("CurrencyAlternateKey", StringType()) \
        .addColumn("CurrencyName", StringType()) \
        .partitionedBy("CurrencyKey") \
        .location(destination_folder) \
        .execute()

    if DeltaTable.isDeltaTable(spark, destination_folder):
        dt_table = DeltaTable.forPath(spark, destination_folder)
        dt_table.alias("historical_data")\
            .merge(
                stream_table.alias("new_data"),
                '''
                historical_data.CurrencyKey = new_data.CurrencyKey 
                ''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        stream_table.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("load_date")\
            .save(destination_folder)

    # write to yugabyte
    stream_table.writeStream \
    .jdbc(YUGABYTEDB_JDBC, destination_table,
          properties={"user": YUGABYTEDB_USER, "password": YUGABYTEDB_PSWD}).mode("overwrite").load()
          
    # write to kafka
    write_into_topic = stream_table \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", destination_topic) \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .start()
        
    # monitoring streaming queries
    # structured streaming output info
    # read last progress & last status of query
    print(write_into_topic.lastProgress)
    print(write_into_topic.status)

    # block until query is terminated
    write_into_topic.awaitTermination()