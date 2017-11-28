import MapReduce

""" 
    This sample code below computes, distributedly, the global word count of 
    a number of texts stored in an input file. 

    The input file book_pages.json stores one pair:
    [title, page_text_as_a_string] per line.

    A word is defined here simply as any non-white-space contiguous string.

    The output is a list of word counts across
    all the texts: 

    ["what", 5]
    ["wheel", 1]
    ["when", 2]
    ...
"""

def mapper(key, value): 
    words = value.split()

    for w in words:
        mr.emit_intermediate(w.lower(), 1)

def reducer(key, list_of_values):
    mr.emit((key, sum(list_of_values)))

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("book_pages.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
