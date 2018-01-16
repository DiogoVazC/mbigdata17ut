"""
To execute on a Farm machine:
time spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 appleStock_farm.py 2> /dev/null
"""
import sys
user = sys.argv[1]

from pyspark import SparkContext
from pyspark.sql import SQLContext

stockFile = "file:///home/" + user + "/aapl-apple-historicalStock.csv"
sc = SparkContext("local", "Stock")

sqlc = SQLContext(sc)
#.option("mode", "DROPMALFORMED") \
stockData = sqlc.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .option("inferschema", 'true') \
    .option("encoding", "UTF-8") \
    .load(stockFile)

stockDataYear = stockData.select("date", "close") \
	.filter(stockData.date >= '2013/01/01') \
    .filter(stockData.date < '2014/12/32')

stockDataYear.printSchema()
print stockDataYear.collect()
