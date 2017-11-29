'''
s1997319 - João David Gonçalves Baião
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value): 
    mr.emit_intermediate(0, max(value))

def reducer(key, list_of_values):
    mr.emit((key, max(list_of_values)))


if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
