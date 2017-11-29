'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value):

    for key in acc:
        mr.emit_intermediate(key, acc[key])

def reducer(key, list_of_values):
    mr.emit((key, sum(list_of_values)))


if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
