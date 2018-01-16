def printFarm(dataframe):
	print dataframe.collect()

def printFarmExample(dataframe, size):
	print dataframe.take(size)

def printClusterRDD(rdd, user, folder):
	rdd.saveAsTextFile("/user/" + user + "/project/data/" + folder)