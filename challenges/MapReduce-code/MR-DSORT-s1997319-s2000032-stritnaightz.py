'''
s1997319 - Joao David Goncalves Baiao
s2000032 - Diogo Antunes Vaz de Carvalho
'''
import MapReduce

def mapper(key, value):
    '''
    Sorting partially sorted lists is much more efficient
    in terms of operations and memory consumption
    than sorting the complete list.
    As each batch of the input has the same length,
    one can consider each batch as one equally-sized sublist to be sorted in the mapper
    '''
    value.sort()
    # Same key for each batch so the reducer can have the whole data at once
    mr.emit_intermediate(0, value)

def reducer(key, list_of_values):
    # Flatten lists received from mapper
    numList = [val for sublist in list_of_values for val in sublist]
    # Sort the whole list. This will be more efficient with previously sorted sublists
    numList.sort()
    for num in numList:
        mr.emit(num)


if __name__ == '__main__':
    data = open("integers.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
