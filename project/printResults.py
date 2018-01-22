def printFarm(dataframe):
	print dataframe.collect()

def printFarmExample(dataframe, size):
	print dataframe.take(size)

def printClusterRDD(rdd, user, folder):
	rdd.saveAsTextFile("/user/" + user + "/project/data/" + folder)

def saveClusterCSV(dataframe, user, folder):
	dataframe.rdd.map(lambda x: ";".join(map(str, x))).coalesce(1).saveAsTextFile("/user/" + user + "/project/data/" + folder + ".csv")
	"""dataframe.write.format("com.databricks.spark.csv").save("/user/" + user + "/project/data/" + folder, header="true")"""