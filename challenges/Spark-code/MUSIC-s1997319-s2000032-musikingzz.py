'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho

----------------------------------------------

Shows visually the distribution of the sales ranks of the products
in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Digital_Music.json.gz

To execute on a Farm machine:
time spark-submit MUSIC-s1997319-s2000032-musikingzz.py 2> /dev/null
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "Music")
sqlc = SQLContext(sc)
df = sqlc.read.json("/data/doina/UCSD-Amazon-Data/meta_Digital_Music.json.gz")
df.printSchema()

'''top_tweets = tweets \
	.flatMap(lambda contents: contents.text) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(lambda a, b: a+b) \
	.sortBy(lambda record: record[1], ascending=False) \
	.take(20)'''
