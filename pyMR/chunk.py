from collections import Iterable


class NotIterableException(Exception):
    pass


class SizeException(Exception):
    pass


class Stack(object):
    """Stack class to help parse JSON like files"""
    def __init__(self, bound_char):
        super(Stack, self).__init__()
        self.bound_char = bound_char
        self._ = []

    def push(self, elem):
        self._.append(elem)

    def pop(self):
        return self.append.pop()


def get_chunks(data, num_chunks, overflow='last'):
    """
    Divides an iterable into chunks

    Args:
        data [iterable]: any iterable object

        num_chunks [int]: number of chunks to be divided

        overflow: 'first' - Add the overflow elements to the
                  first chunk

                  'last' - Add the overflow elements to the
                  last chunk

    Return:
        chunks - a list of divided parts of the data
    """

    if not isinstance(data, Iterable):
        raise NotIterableException("'data' should be iterable")

    try:
        len_ = len(data)
    except:
        len_ = sum([1 for _ in data])

    if len_ < num_chunks:
        raise SizeException("len(data) should be more than num_chunks")

    if not isinstance(num_chunks, int):
        raise Exception("'num_chunks' must be of type 'int'")

    chunks = []

    overflow_len = len_ % num_chunks
    each_chunk = len_ // num_chunks

    temp_chunk = []
    for i, elem in enumerate(data, start=1):
        temp_chunk.append(elem)

        if not i % each_chunk and len(chunks) < num_chunks:
            chunks.append(temp_chunk)
            temp_chunk = []

        if i == len_:
            assert len(temp_chunk) == overflow_len
            if overflow == 'first':
                chunks[0] += temp_chunk
            else:
                chunks[-1] += temp_chunk

    return chunks
