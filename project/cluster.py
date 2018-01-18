"""
This is the main file for cluster.
spark-submit --master yarn --deploy-mode cluster cluster.py [function] [user] [folder] [company] [unixBeginTime] [unixEndTime] ['day'/'10days'/'month'] 
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime
import dataframeOperations as operation
import printResults as printR
import consts
import index

"""Get Arguments"""
import sys
fc = sys.argv[1]
consts.user = sys.argv[2]
consts.folder = sys.argv[3]
consts.company = sys.argv[4]
consts.beginTime = sys.argv[5] if (len(sys.argv) > 6) else consts.Jan2013
consts.endTime = sys.argv[6] if (len(sys.argv) > 6) else consts.Jun2013
consts.timeframe = sys.argv[7] if (len(sys.argv) > 7) else 'day'

"""Initialize Spark"""
sc = SparkContext(appName="Stock Value")
sqlc = SQLContext(sc)

result = index.index[fc](sqlc)
