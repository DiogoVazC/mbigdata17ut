"""
This computes the most frequent 50 words across the 
books in the HDFS folder /data/doina/Gutenberg-EBooks.

To execute on a Farm machine:
time spark-submit word_count.py 2> /dev/null
"""

from pyspark import SparkContext

sc = SparkContext("local", "Word count")
sc.setLogLevel("ERROR")

path = "/data/doina/Gutenberg-EBooks"
rdd = sc.wholeTextFiles(path)

top_words = rdd \
	.flatMap(lambda (file, contents): contents.lower().split()) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(lambda a, b: a+b) \
	.sortBy(lambda record: record[1], ascending=False) \
	.take(50)

for (w, c) in top_words:
	print "Word:\t", w, "\toccurrences:\t", c
