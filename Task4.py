import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from shapely.geometry import Polygon, Point
import argparse

parser = argparse.ArgumentParser(description='Trending Arrivals')
parser.add_argument('--input', required=False, help='Input data directory')
parser.add_argument('--checkpoint', required=False, help='checkpoint directory')
parser.add_argument('--output', required=False, help='Output directory')

args = parser.parse_args()

inputPath = args.input if args.input is not None else 'taxi-data'
outputPath = args.output if args.output is not None else 'output'
checkpoint_path = args.checkpoint if args.checkpoint is not None else 'checkpoint'

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
convertUDF = udf(lambda z: z)

streamingCountsDF = (
    streamingInputDF
    .withColumn("lat", when(col("type") == "yellow", convertUDF(col("Yellow_Dropoff_latitude"))).otherwise(
        convertUDF(col("Green_Dropoff_latitude"))))
    .withColumn("long", when(col("type") == "yellow", convertUDF(col("Yellow_Dropoff_longitude"))).otherwise(
        convertUDF(col("Green_Dropoff_longitude"))))

    .withColumn("headquarters", classify_udf(col("long"), col("lat")))
    .groupBy(
        window(streamingInputDF.Lpep_dropoff_datetime, "10 minutes").alias("window"),

        "headquarters"
    )
    .count()
    .withColumn("timestamp", hour(col("window").end) * 3600 + minute(col("window").end) * 60)
)


def foreach_batch_function(df, id):
    print("At " + str(id))
    outFileName = "part-"
    # Transform and write batchDF
    df1 = (df.withColumnRenamed("timestamp", "timestamp_1").withColumnRenamed("headquarters", "headquarters_1")
           .withColumnRenamed("count", "count_1"))
    df2 = df.join(df1,
                  (df['timestamp'] - df1['timestamp_1'] == 600) &
                  (df['headquarters'] != "none") &
                  (df['headquarters'] == df1['headquarters_1']) &
                  (df['count'] >= 10) &
                  (df['count'] - df1['count_1'] >= df1["count_1"]),
                  how="inner")
    rows = df2.select("timestamp").collect()

    for row in rows:
        filename = outputPath + "/" + outFileName + str((24 if int(row[0]) == 0 else int(row[0])) * 100)
        with open(filename, "w") as f:
            try:
                count_val = df2.select("count").where(col("headquarters") == "citigroup").collect()[0][0]
                time_val = df2.select("timestamp").where(col("headquarters") == "citigroup").collect()[0][0]
                count_1_val = df2.select("count_1").where(col("headquarters") == "citigroup").collect()[0][0]
                f.write("(citigroup, ({}, {}, {}))".format(str(count_val), str(time_val), str(count_1_val)))
                print("The number of arrivals to Citigroup has doubled from {} to {} at {}!"
                      .format(str(count_val), str(time_val), str(count_1_val)))
            except IndexError:
                # print("No data found for Citigroup")
                f.write(" ")
            except Exception as ea:
                f.write(" ")
                # print(f"Error occurred: {ea}")

            try:
                count_val = df2.select("count").where(col("headquarters") == "goldman").collect()[0][0]
                time_val = df2.select("timestamp").where(col("headquarters") == "goldman").collect()[0][0]
                count_1_val = df2.select("count_1").where(col("headquarters") == "goldman").collect()[0][0]
                f.write("(goldman, ({}, {}, {}))".format(str(count_val), str(time_val), str(count_1_val)))
                print("The number of arrivals to Goldman Sachs has doubled from {} to {} at {}!"
                      .format(str(count_val), str(time_val), str(count_1_val)))
            except IndexError:
                f.write(" ")
            except Exception as ea:
                f.write(" ")


query = (
    streamingCountsDF
    .writeStream
    .foreachBatch(foreach_batch_function)  # complete = all the counts should be in the tabl
    .outputMode("update")
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
