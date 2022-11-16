# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, current_date, col, lit, concat
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import IntegerType, StringType

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("example-geography-silver-py") \
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
    address_bronze = "s3a://lakehouse/bronze/example/address/"
    salesterritory_silver = "s3a://lakehouse/silver/example/dimsalesterritory/"

    destination_folder = "s3a://lakehouse/silver/example/dimgeography/"
    write_delta_mode = "overwrite"
    # read bronze data

    address_df = spark.read.format("delta").load(address_bronze)
    salesterritory_df = spark.read.format("delta").load(salesterritory_silver)

    indexer_statecode = StringIndexer(inputCol="StateProvince", outputCol="StateProvinceCode")
    address_df = indexer_statecode.fit(address_df).transform(address_df)

    address_df = address_df.alias("a")
    salesterritory_df = salesterritory_df.alias("st")

    silver_table = (
        address_df
        .join(salesterritory_df,col("a.CountryRegion")==col("st.SalesTerritoryCountry"),"left")
        .select(
            col("a.AddressID").alias("GeographyKey"),
            col("a.City").alias("City"),
            col("a.StateProvinceCode").alias("StateProvinceCode"),
            col("a.StateProvince").alias("StateProvinceName"),
            col("st.SalesTerritoryKey").alias("CountryRegionCode"),
            col("st.SalesTerritoryCountry").alias("EnglishCountryRegionName"),
            lit(None).alias("SpanishCountryRegionName"),
            lit(None).alias("FrenchCountryRegionName"),
            col("a.PostalCode").alias("PostalCode"),
            col("st.SalesTerritoryKey").alias("SalesTerritoryKey"),
            lit(None).alias("IpAddressLocator")
    )
    )

    silver_table = silver_table.withColumn("s_create_at", current_timestamp())
    silver_table = silver_table.withColumn("s_load_date", current_date())

   
    if DeltaTable.isDeltaTable(spark, destination_folder):
        dt_table = DeltaTable.forPath(spark, destination_folder)
        dt_table.alias("historical_data")\
            .merge(
                silver_table.alias("new_data"),
                '''
                historical_data.AddressID = new_data.AddressID 
                ''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        silver_table.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("s_load_date")\
            .save(destination_folder)

    #verify count origin vs destination
    origin_count = silver_table.count()

    destiny = spark.read \
        .format("delta") \
        .load(destination_folder)
    
    destiny_count = destiny.count()

    if origin_count != destiny_count:
        raise AssertionError("Counts of origin and destiny are not equal")
    
    # stop session
    spark.stop()