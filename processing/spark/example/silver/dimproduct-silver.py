# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, current_date, col, lit, when

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("example-dimproduct-silver") \
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
    product_bronze = "s3a://lakehouse/bronze/example/product/"
    productcategory_bronze = "s3a://lakehouse/bronze/example/productcategory/"

    destination_folder = "s3a://lakehouse/silver/example/dimproduct/"
    write_delta_mode = "overwrite"
    # read bronze data

    product_df = spark.read.format("delta").load(product_bronze)
    product_df = product_df.alias("p")

    silver_table = (
        product_df
        .join(customeraddress_df, col("c.CustomerID")==col("ca.CustomerID"),"left")
        .join(address_df,col("a.AddressID")==col("ca.AddressID"),"left")
        .select(
            col("p.ProductID").alias("ProductKey"),
            col("p.ProductNumber").alias("ProductAlternateKey"),
            col("c.rowguid").alias("ProductSubcategoryKey"),
            col("c.Title").alias("WeightUnitMeasureCode"),
            col("c.FirstName").alias("SizeUnitMeasureCode"),
            col("c.MiddleName").alias("EnglishProductName"),
            col("c.LastName").alias("SpanishProductName"),
            col("c.NameStyle").alias("FrenchProductName"),
            col("c.Suffix").alias("StandardCost"),
            col("c.Suffix").alias("FinishedGoodsFlag"),
            col("c.EmailAddress").alias("Color"),
            lit(None).alias("SafetyStockLevel"),
            lit(None).alias("ReorderPoint"),
            lit(None).alias("ListPrice"),
            lit(None).alias("Size"),
            lit(None).alias("SizeRange"),
            lit(None).alias("Weight"),
            lit(None).alias("DaysToManufacture"),
            lit(None).alias("ProductLine"),
            lit(None).alias("DealerPrice"),
            lit(None).alias("Class"),
            lit(None).alias("Style"),
            col("a.AddressLine1").alias("ModelName"),
            col("a.AddressLine2").alias("LargePhoto"),
            col("c.Phone").alias("EnglishDescription"),
            lit(None).alias("FrenchDescription"),
            lit(None).alias("ChineseDescription"),
            lit(None).alias("ArabicDescription"),
            lit(None).alias("HebrewDescription"),
            lit(None).alias("ThaiDescription"),
            lit(None).alias("GermanDescription"),
            lit(None).alias("JapaneseDescription"),
            lit(None).alias("TurkishDescription"),
            lit(None).alias("StartDate"),
            lit(None).alias("EndDate"),
            lit(None).alias("Status")
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
                historical_data.CustomerID = new_data.CustomerID 
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