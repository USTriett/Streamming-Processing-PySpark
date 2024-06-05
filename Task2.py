from pyspark.python.pyspark.shell import spark
from pyspark.sql import dataframe
from pyspark.sql.functions import window, date_format, from_unixtime, col, hour
from pyspark.sql.types import *

inputPath = "taxi-data"
outputPath = "output"
# Define the schema to speed up processing


# yellowSchema = StructType([
#     StructField("type", StringType(), True),
#     StructField("VendorID", IntegerType(), True),
#     StructField("tpep_pickup_datetime", TimestampType(), True),
#     StructField("tpep_dropoff_datetime", TimestampType(), True),
#     StructField("passenger_count", IntegerType(), True),
#     StructField("trip_distance", FloatType(), True),
#     StructField("pickup_longitude", FloatType(), True),
#     StructField("pickup_latitude", FloatType(), True),
#     StructField("RatecodeID", IntegerType(), True),
#     StructField("store_and_fwd_flag", StringType(), True),
#     StructField("dropoff_longitude", FloatType(), True),
#     StructField("dropoff_latitude", FloatType(), True),
#     StructField("payment_type", IntegerType(), True),
#     StructField("fare_amount", FloatType(), True),
#     StructField("extra", FloatType(), True),
#     StructField("mta_tax", FloatType(), True),
#     StructField("tip_amount", FloatType(), True),
#     StructField("tolls_amount", FloatType(), True),
#     StructField("improvement_surcharge", FloatType(), True),
#     StructField("total_amount", FloatType(), True)
# ])
#
# greenSchema = StructType([
#     StructField("type", StringType(), True),
#     StructField("VendorID", IntegerType(), True),
#     StructField("lpep_pickup_datetime", TimestampType(), True),
#     StructField("Lpep_dropoff_datetime", TimestampType(), True),
#     StructField("Store_and_fwd_flag", StringType(), True),
#     StructField("RateCodeID", IntegerType(), True),
#     StructField("Pickup_longitude", FloatType(), True),
#     StructField("Pickup_latitude", FloatType(), True),
#     StructField("Dropoff_longitude", FloatType(), True),
#     StructField("Dropoff_latitude", FloatType(), True),
#     StructField("Passenger_count", IntegerType(), True),
#     StructField("Trip_distance", FloatType(), True),
#     StructField("Fare_amount", FloatType(), True),
#     StructField("Extra", FloatType(), True),
#     StructField("MTA_tax", FloatType(), True),
#     StructField("Tip_amount", FloatType(), True),
#     StructField("Tolls_amount", FloatType(), True),
#     StructField("Ehail_fee", FloatType(), True),
#     StructField("improvement_surcharge", FloatType(), True),
#     StructField("Total_amount", FloatType(), True),
#     StructField("Payment_type", IntegerType(), True),
#     StructField("Trip_type", IntegerType(), True)
# ])

csvSchema = StructType([
    StructField("type", StringType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("Lpep_dropoff_datetime", TimestampType(), True),
    StructField("Store_and_fwd_flag", StringType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("Pickup_longitude", FloatType(), True),
    StructField("Pickup_latitude", FloatType(), True),
    StructField("Dropoff_longitude", FloatType(), True),
    StructField("Dropoff_latitude", FloatType(), True),
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

streamingInputDF = (
    spark
    .readStream  # Set the schema of the JSON data
    .schema(csvSchema)
    .option("maxFilesPerTrigger", 60)  # Treat a sequence of files as a stream by picking one file at a time
    .csv(inputPath)
)

streamingCountsDF = (
    streamingInputDF
    .groupBy(
        window(streamingInputDF.Lpep_dropoff_datetime, "1 hours").alias("window")
    )
    .count()

).withColumn("hour", hour(col("window").end))


def foreach_batch_function(df, id):
    outFileName = "output-"
    # Transform and write batchDF
    rows = df.select("hour").collect()

    for row in rows:
        filename = outputPath + "/" + outFileName + str((24 if int(row[0]) == 0 else int(row[0])) * 360000)
        with open(filename, "w") as f:
            count = df.select("count").where(col("hour") == row[0]).collect()[0][0]
            f.write(str(count))


query = (
    streamingCountsDF
    .writeStream
    .foreachBatch(foreach_batch_function)  # complete = all the counts should be in the tabl
    .outputMode("update")
    .start()
)

query.awaitTermination(10)
