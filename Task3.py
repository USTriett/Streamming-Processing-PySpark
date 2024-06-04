import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from shapely.geometry import Polygon, Point


inputPath = "taxi-data"
outputPath = "output"

spark = SparkSession.builder \
    .appName("Streaming Example") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

csvSchema = StructType([
    StructField("type", StringType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("Lpep_dropoff_datetime", TimestampType(), True),
    StructField("Store_and_fwd_flag", StringType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("Pickup_longitude", FloatType(), True),
    StructField("Pickup_latitude", FloatType(), True),
    StructField("Green_Dropoff_longitude", FloatType(), True),
    StructField("Green_Dropoff_latitude", FloatType(), True),
    StructField("Yellow_Dropoff_longitude", FloatType(), True),
    StructField("Yellow_Dropoff_latitude", FloatType(), True),
    StructField("Passenger_count", IntegerType(), True),
    StructField("Trip_distance", FloatType(), True),
    StructField("Fare_amount", FloatType(), True),
    StructField("Extra", FloatType(), True),
    StructField("MTA_tax", FloatType(), True),
    StructField("Tip_amount", FloatType(), True),
    StructField("Tolls_amount", FloatType(), True),
    StructField("Ehail_fee", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("Total_amount", FloatType(), True),
    StructField("Payment_type", IntegerType(), True),
    StructField("Trip_type", IntegerType(), True)
])

def inarea(val1, val2, area):
    polygon = Polygon(area)
    point = Point(val1, val2)
    return polygon.contains(point)

inarea_udf = udf(inarea, BooleanType())

goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140, 40.720053], [-74.012083, 40.720267]]

streamingInputDF = (
    spark
    .readStream
    .schema(csvSchema)
    .option("maxFilesPerTrigger", 1)  
    .csv(inputPath) 
)

# filteredStreamingDF = streamingInputDF.filter(
#     # inarea_udf(streamingInputDF.Yellow_Dropoff_longitude, streamingInputDF.Yellow_Dropoff_latitude, goldman_bbox) |
#     # inarea_udf(streamingInputDF.Yellow_Dropoff_longitude, streamingInputDF.Yellow_Dropoff_latitude, citigroup_bbox) |
#     # inarea_udf(streamingInputDF.Green_Dropoff_longitude, streamingInputDF.Green_Dropoff_latitude, goldman_bbox) |
#     # inarea_udf(streamingInputDF.Green_Dropoff_longitude, streamingInputDF.Green_Dropoff_latitude, citigroup_bbox)
    
# )

# streamingInputDF = streamingInputDF.withColumn("headquarters",
#                                                      when(inarea_udf(streamingInputDF.Yellow_Dropoff_longitude,
#                                                                      streamingInputDF.Yellow_Dropoff_latitude,
#                                                                      goldman_bbox) |
#                                                           inarea_udf(streamingInputDF.Green_Dropoff_longitude,
#                                                                      streamingInputDF.Green_Dropoff_latitude,
#                                                                      goldman_bbox), "goldman")
#                                                      .otherwise("citigroup"))

# streamingCountsDF = (
#     filteredStreamingDF
#     .groupBy(window(filteredStreamingDF.Lpep_dropoff_datetime, "1 hour").alias("window"),
#              col("headquarters"))
#     .count()
#     .withColumn("hour", hour(col("window").end))
# )

# def foreach_batch_function(df, id):
#     for row in df.collect():
#         outFileName = f"output-{int(row['hour']) * 360000}-{row['headquarters']}"
#         with open(outFileName, "w") as f:
#             f.write(f"{row['headquarters']}, {row['count']}")

# query = (
#     streamingCountsDF
#     .writeStream
#     .foreachBatch(foreach_batch_function)
#     .outputMode("update")
#     .start()
# )

# query.awaitTermination()

def inarea(val1, val2, area):
    polygon = Polygon(area)

    point = Point(val1, val2)
    return polygon.contains(point)


def classify(val1, val2):
    if inarea(val1, val2, goldman):
        return 'goldman'
    if inarea(val1, val2, citigroup):
        return 'citigroup'
    return 'none'

values = []

# def create_headquarter_column(type ,ylat, ylng, glat, glng):

#     print(type)
#     if type == "yellow":
#        values.append(classify(ylat, ylng))
#        print(values[-1])

#     else:
#        values.append(classify(glat, glng))
#        print(values[-1])
            
            
#     return 


# create_headquarter_udf = udf(create_headquarter_column, ArrayType(StringType()))
classify_udf = udf(classify, StringType())

streamingCountsDF = (
    streamingInputDF
    .withColumn("headquarters", when(col("type") == "yellow", classify_udf(col("Yellow_Dropoff_latitude"), col("Yellow_Dropoff_longitude")))\
                .otherwise(classify_udf(col("Green_Dropoff_latitude"), col("Green_Dropoff_longitude")))        
        )
    .groupBy(
        window(streamingInputDF.Lpep_dropoff_datetime, "1 hours").alias("window"),
        "headquarters"
    )
    .count()
    .withColumn("hour", hour(col("window").end))
)

# def foreach_batch_function(df, id):
#     outFileName = "output-"
#     # Transform and write batchDF
#     rows = df.select("hour").collect()
#
#     for row in rows:
#         filename = outputPath + "/" + outFileName + str(int(row[0]) * 360000)
#         with open(filename, "w") as f:
#             count = df.select("count").where(col("hour") == row[0]).collect()[0][0]
#             f.write(str(count))


query = (
    streamingCountsDF
    .writeStream
    .format("console")
    .outputMode("complete")
    .start()
)

query.awaitTermination()