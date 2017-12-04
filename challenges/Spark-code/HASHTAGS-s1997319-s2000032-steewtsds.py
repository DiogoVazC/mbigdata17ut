'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho

----------------------------------------------

This computes the most frequent 20 hashtags across the
tweets in the HDFS file /data/doina/twitterNL/201612/20161231-23.out.gz

To execute on a Farm machine:
time spark-submit HASHTAGS-s1997319-s2000032-steewtsds.py 2> /dev/null
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext("local", "Twitter")
sqlc = SQLContext(sc)
df = sqlc.read.json("/data/doina/twitterNL/201612/20161231-23.out.gz")
tweets = df.select("entities.hashtags.text")
tweets.printSchema()

top_tweets = tweets \
	.flatMap(lambda contents: contents.text) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(lambda a, b: a+b) \
	.sortBy(lambda record: record[1], ascending=False) \
	.take(20)

for (w, c) in top_tweets:
	print "Hashtag:\t", w, "\toccurrences:\t", c
