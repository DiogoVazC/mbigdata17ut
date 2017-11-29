'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value):
    words = value.split()
    acc = dict()

    for w in words:
        acc[w.lower()] = (acc[w.lower()] + 1) if (w.lower() in acc) else 1

    for i in acc:
        mr.emit_intermediate(i, acc[i])

def reducer(key, list_of_values):
    mr.emit((key, sum(list_of_values)))


if __name__ == '__main__':
    data = open("book_pages.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
