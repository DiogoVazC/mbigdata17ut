"""
Cluster:
spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn --deploy-mode cluster appleStock_cluster.py
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext

stockFile = "file:///home/s2000032/aapl-apple-historicalStock.csv"
sc = SparkContext(appName="Amazon Products")

sqlc = SQLContext(sc)
#.option("mode", "DROPMALFORMED") \
stockData = sqlc.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .option("inferschema", 'true') \
    .option("encoding", "UTF-8") \
    .load(stockFile)

stockData.printSchema()
