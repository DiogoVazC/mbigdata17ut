import json 
import gzip 

def parse(path): 
	g = gzip.open('ratings_Electronics.csv', 'r') 
	for l in g: 
		yield json.dumps(eval(l)) 

	f = open("output.strict", 'w') 
	for l in parse("ratings_Electronics.csv"):
		f.write(l + '\n')