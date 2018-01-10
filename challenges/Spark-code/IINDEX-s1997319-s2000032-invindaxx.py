'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho

----------------------------------------------

This computes the words that are contained in all the books
stored in /data/doina/Gutenberg-EBooks

To execute on a Farm machine:
time spark-submit IINDEX-s1997319-s2000032-invindaxx.py 2> /dev/null
'''

from pyspark import SparkContext

sc = SparkContext("local", "Books")
books = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")
num_books = books.count()

all_words = books \
	.flatMap(lambda (path, content): map(lambda word: (''.join(x for x in word if x.isalpha()).lower(), [path]), content.split())) \
	.reduceByKey(lambda a, b: list(set(a)|set(b))) \
	.filter(lambda (word, books): len(books) == num_books) \
	.sortBy(lambda record: record[0], ascending=True) \
	.collect()

for (w, b) in all_words:
	print w,
