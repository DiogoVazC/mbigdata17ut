'''
s1997319 - João David Gonçalves Baião
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value): 
    # print("key: ", key, " value: ", value)
    mr.emit_intermediate(0, max(value))

def reducer(key, list_of_values):
    # print("key: ", key, " value: ", list_of_values)
    mr.emit((key, max(list_of_values)))

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
