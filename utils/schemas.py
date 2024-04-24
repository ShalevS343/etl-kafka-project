from redisearch import TextField, TagField
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Schemas:
    
    REDIS_SCHEMA = [
        TextField('imdb_id'),
        TextField('movie_name', sortable=True),
        TagField('genres'),
        TagField('directors'),
        TagField('lead_actors'),
        TextField('rating', sortable=True),
        TagField('awards'),
        TextField('release_date', sortable=True)
    ]

    PYSPARK_SCHEMA = StructType([
        StructField("imdb_id", StringType(), True),
        StructField("movie_name", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("lead_actors", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("awards", StringType(), True),
        StructField("release_date", StringType(), True),
        
        # The amount of times a row was touched
        StructField("touch_counter", IntegerType(), True)
    ])
