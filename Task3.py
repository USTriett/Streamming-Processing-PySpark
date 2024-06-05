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


goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140, 40.720053], [-74.012083, 40.720267]]

streamingInputDF = (
    spark
    .readStream
    .schema(csvSchema)
    .option("maxFilesPerTrigger", 60)
    .csv(inputPath)
)


def inarea(val1, val2, area):
    polygon = Polygon(area)
    print([val1, val2])
    point = Point(val1, val2)
    return polygon.contains(point)


def classify(val1, val2):
    if inarea(val1, val2, goldman):
        return 'goldman'
    if inarea(val1, val2, citigroup):
        return 'citigroup'
    return 'none'


# create_headquarter_udf = udf(create_headquarter_column, ArrayType(StringType()))
classify_udf = udf(classify, StringType())
convertUDF = udf(lambda z: z)

streamingCountsDF = (
    streamingInputDF
    .withColumn("lat", when(col("type") == "yellow", convertUDF(col("Yellow_Dropoff_latitude"))).otherwise(
        convertUDF(col("Green_Dropoff_latitude"))))
    .withColumn("long", when(col("type") == "yellow", convertUDF(col("Yellow_Dropoff_longitude"))).otherwise(
        convertUDF(col("Green_Dropoff_longitude"))))

    .withColumn("headquarters", classify_udf(col("long"), col("lat")))
    .groupBy(
        window(streamingInputDF.Lpep_dropoff_datetime, "1 hours").alias("window"),

        "headquarters"
    )
    .count()
    .withColumn("hour", hour(col("window").end))
)


def foreach_batch_function(df, id):
    outFileName = "output3-"
    # Transform and write batchDF
    rows = df.select("hour").collect()
    for row in rows:
        filename = outputPath + "/" + outFileName + str((24 if int(row[0]) == 0 else int(row[0])) * 360000)

        with open(filename, "w") as f:
            citygroup_count = df.select("count").where(col("headquarters") == "citigroup").collect()[0][0]
            goldman_count = df.select("count").where(col("headquarters") == "goldman").collect()[0][0]
            f.write(str(("citigroup", citygroup_count)) + "\n" + str(("goldman", goldman_count)))


query = (
    streamingCountsDF
    .writeStream
    .foreachBatch(foreach_batch_function)  # complete = all the counts should be in the tabl
    .outputMode("update")
    .start()
)

query.awaitTermination()
