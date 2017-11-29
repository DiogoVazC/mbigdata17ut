'''
s1997319 - João David Gonçalves Baião
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value):
	text = value.get("text");
	hashtags = []
	for word in text.split():
		if (word.startswith('#')):
			hashtags.append(word)

	for h in hashtags:
		mr.emit_intermediate(h, 1)

def reducer(key, list_of_values):
    total = sum(list_of_values)
    if total >= 20:
    	mr.emit((key, total))


if __name__ == '__main__':
    data = open("one_hour_of_tweets.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
