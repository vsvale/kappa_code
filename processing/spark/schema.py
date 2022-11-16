from pyspark.sql.types import *

schema = StructType(
    [
        StructField('user_id',IntergerType(),True),
        StructField('title',StringType(),True),
        StructField('dt_time',TimeStampType(),True),

    ]
)