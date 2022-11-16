
# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, current_date
import databricks.koalas as ks
import pandas as pd
import unicodedata


def main():
    spark = SparkSession \
        .builder \
        .appName("diesel-to-landing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.0.2:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "LBGipuLnLkJgwjXE") \
        .config("spark.hadoop.fs.s3a.secret.key", "WjnPIFgPIhsjeFSgyvo0vurlMNfjDWyV") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.memory.offHeap.enabled","true")  \
        .config("spark.memory.offHeap.size","100mb") \
        .getOrCreate()

    # Set log level to info
    spark.sparkContext.setLogLevel('INFO')

    # [extraction]
    # diesel df from url

    df_diesel_raw = pd.read_csv("https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2022.csv"
                            , delimiter=";")

    # [adjusting column names]
    new_column_names = [unicodedata.normalize('NFKD', i.replace(" ", "_")).encode('ascii', 'ignore').decode() for i in df_diesel_raw.columns]

    # [converting pandas to spark]
    df_diesel_raw.columns = new_column_names

    kdf_diesel_raw = ks.from_pandas(df_diesel_raw)

    sdf_diesel_raw = kdf_diesel_raw.to_spark()

    sdf_diesel_raw = sdf_diesel_raw.withColumn("created_at", current_timestamp())
    sdf_diesel_raw = sdf_diesel_raw.withColumn("load_date", current_date())

    sdf_diesel_raw.write.format("jdbc")\
        .option("url","jdbc:postgresql://yb-tservers.database.svc.cluster.local:5433/owshq?user=plumber&password=PlumberSDE")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "diesel_raw")\
        .option("mode", "overwrite")\
        .save()


    # [write to lake]
    # [landing area]
#    write_mode = "overwrite"
#    landing_path = "s3a://lakehouse"
#
#    sdf_diesel_raw.write.mode(write_mode)\
#        .format('com.databricks.spark.csv')\
#        .option("header", "true")\
#        .partitionBy("load_date")\
#        .save(landing_path + "/landing/diesel/vendas-oleo-diesel-tipo-m3-2013-2022.csv")

    # stop session
    spark.stop()

if __name__ == '__main__':
    main()