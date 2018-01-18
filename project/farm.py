"""
This is the main file for farm.
only amazon:
time spark-submit farm.py [function] [user] [companyName] [unixBeginTime] [unixEndTime] [day/10days/month] 2> /dev/null

if stock:
time spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 farm.py [function] [user] [company] [unixBeginTime] [unixEndTime] 2> /dev/null
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime
import dataframeOperations as operation
import printResults as printR
import consts
import index_farm

"""Get Arguments"""
import sys
fc = sys.argv[1]
consts.user = sys.argv[2]
consts.company = sys.argv[3]
consts.beginTime = sys.argv[4] if (len(sys.argv) > 5) else consts.Jan2013
consts.endTime = sys.argv[5] if (len(sys.argv) > 5) else consts.Jun2013
consts.timeframe = sys.argv[6] if (len(sys.argv) > 6) else 'day'

"""Initialize Spark"""
sc = SparkContext("local", "Stock Value")
sqlc = SQLContext(sc)

print "index_farm_call"
result = index_farm.index[fc](sqlc)