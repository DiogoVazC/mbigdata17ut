import MapReduce

""" This computes, distributedly, the inverted index of a number of text pages stored 
    in a single file. The output shows, for all words in all texts, 
    which titles you can find that word in.

    The input file book_pages.json stores one pair:
    [title, page_text_as_a_string] per line.

    A word is defined simply as any non-white-space contiguous string.

    In the output, any given title should appear only once per word:
    ["was", ["austen-emma.txt", "bible-kjv.txt", "melville-moby_dick.txt"]], 
    ["water", ["blake-poems.txt"]], etc.
"""

def mapper(key, value): 
    # without a combiner
    words = value.split()
    for w in words:
        mr.emit_intermediate(w.lower(), key)

def reducer(key, list_of_values):
    res = list(set(list_of_values))
    res.sort()        # this is small data, so I can sort for a pretty output
    mr.emit((key, res))

# ____________________________________________________________________________
# This code remains unmodified in all programs, except for the input file name.

if __name__ == '__main__':
    data = open("book_pages.json")
    mr = MapReduce.MapReduce()
    mr.execute(data, mapper, reducer)
