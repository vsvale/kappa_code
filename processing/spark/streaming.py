#set default location for [spark-warehouse]
warehouse-location = abspath('spark-warehouse')

# main spark program

    # init spark session
    spark = (Sparksession
    .builder
    .appName('movies-by-user-stream')
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')
    .config('spark.sql.warehouse.dir',warehouse_location)
    .enableHiveSupport()
    .getOrCreate()
    )

    #set log info
    spark.sparkContext.setloglevel('INFO')

    #schema is defined in schema.py
    schema = jsc_movies_titles

    jsonOptions = {'timestampFormat':"yyyy-MM-dd'T'HH:mm:ss.sss'Z'"}

    #variables defined in setting.py
    print(BOOTSTRAP_SERVERS)
    print(INPUT_MOVIES_TITLES_TOPIC)
    print(OUTPUT_TOPIC_MOVIES_BY_USERS_STREAM)
    print(STARTING_OFFSETS)

    #read from kafka, json output
    strem_movies_titles = (spark
    .readStream
    .format('kafka')
    .option('kafka.bootstrap.servers',BOOTSTRAP_SERVERS)
    .option('subscribe',INPUT_MOVIES_TITLES_TOPIC)
    .option('startingOffsets',STARTING_OFFSETS)
    .option('checkpoint',checkpoint)
    .load()
    .select(from_json(
        col('value').cast('string'),
        sch_movies_titles,
        jsonOptions
        ).alias('movies_titles')
        )
    )

    stream_movies_titles.printSchema()

write_into_topic = (stream_movies_titles
.select(to_json(struct(col('user_id'),col('genres'))))
.writeStream
.format('console')
.start()
)
print(write_into_topic)

write_into_topic = (stream_movies_titles
.select(to_json(struct(col('user_id'),col('genres'))))
.writeStream
.format('kafka')
.option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)
.option('topic',OUTPUT_TOPIC_MOVIES_BY_USERS_STREAM)
.option('checkpointlocation',checkpoint)
.outputMode('append')
.start()
)
write_into_topic.awaitTermination()