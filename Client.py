from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")


sparkContext = SparkContext(
    conf=conf
)

sparkContext.setLogLevel("ERROR")

streamingContext = StreamingContext(sparkContext, 1)

textStreamLines = streamingContext.socketTextStream(
    hostname="localhost",
    port=10002,
)

words = textStreamLines.flatMap(lambda line: line.split(" "))
wordsDF = words.map(lambda word: (word, 1))
wordCountsDF = wordsDF.reduceByKey(lambda x, y: x + y)

wordCountsDF.pprint()
streamingContext.start()
streamingContext.awaitTermination()
