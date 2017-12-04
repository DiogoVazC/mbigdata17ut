'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho

----------------------------------------------

This computes the most frequent 40 tweeters across the
tweets in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Books_sample.json.gz

To execute on a Farm machine:
time spark-submit USERS-s1997319-s2000032-boookingzasd.py 2> /dev/null
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext

filename = '/data/doina/UCSD-Amazon-Data/meta_Books_sample.json.gz'
sc = SparkContext("local", "Books")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

books = df.select("related") \
	.filter("related.also_bought is not NULL")

books.printSchema()

frequent = books \
	.flatMap(lambda contents: [(x, 1) for x in contents.related.also_bought]) \
	.reduceByKey(lambda a, b: a+b) \
	.sortBy(lambda record: record[1], ascending=False) \
	.take(1)

print "asin:\t",frequent[0][0], "\talso bought with\t", frequent[0][1], " other titles"
most_frequent = df.select("asin", "title", "price", "imUrl","related", "salesRank", "categories") \
	.where(df.asin == frequent[0][0]).collect()

print most_frequent
