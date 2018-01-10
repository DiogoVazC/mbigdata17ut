'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho

----------------------------------------------

Shows visually the distribution of the sales ranks of the products
in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Digital_Music.json.gz

To execute on a Farm machine:
time spark-submit MUSIC-s1997319-s2000032-musikingzz.py 2> /dev/null
'''

import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt # For histogram plot
from pyspark import SparkContext
from pyspark.sql import SQLContext

bucket_size = 1000
sc = SparkContext("local", "Music")
sqlc = SQLContext(sc)
df = sqlc.read.json("/data/doina/UCSD-Amazon-Data/meta_Digital_Music.json.gz")
salesRanks = df.select("salesRank.Music").filter("Music is not null")

sales_buckets = salesRanks \
	.flatMap(lambda content: content) \
	.map(lambda rank: (rank/bucket_size, 1)) \
	.reduceByKey(lambda a, b: a+b) \
	.sortBy(lambda record: record[0], ascending=True) \
	.collect()

# Histogram plot
bins = [record[0] for record in sales_buckets]
count = [record[1] for record in sales_buckets]
plt.title("Sales ranks for Amazon Music")
plt.xlabel("Bucket (" + str(bucket_size) + " sales ranks each)")
plt.ylabel("Count of products / bucket")
plt.bar(bins, count)
plt.savefig('MUSIC-s1997319-s2000032-musikingzz.png')

print bucket_size
print count
