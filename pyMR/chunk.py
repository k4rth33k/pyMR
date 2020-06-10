# root/pyMR/chunk.py

from collections import Iterable
from itertools import tee


class NotIterableException(Exception):
    pass


class SizeException(Exception):
    pass


class Chunks():
    def __init__(self, data, num_chunks, overflow='last'):
        self.data, self.len_data = tee(data)
        self.num_chunks = num_chunks
        self.overflow = overflow
        self.chunks = []
        self.chunk_count = 0

        if not isinstance(data, Iterable):
            raise NotIterableException("'data' should be iterable")

        try:
            self.len_ = len(data)
        except TypeError:
            self.len_ = sum([1 for _ in self.len_data])

        if self.len_ < self.num_chunks:
            raise SizeException("len(data) should be more than num_chunks")

        if not isinstance(self.num_chunks, int):
            raise Exception("'num_chunks' must be of type 'int'")

        self.overflow_len = self.len_ % self.num_chunks
        self.each_chunk = self.len_ // self.num_chunks

    def get_chunks(self):

        temp_chunk = []
        for i, elem in enumerate(self.data, start=1):
            temp_chunk.append(elem)

            if not i % self.each_chunk and len(self.chunks) < self.num_chunks:
                self.chunks.append(temp_chunk)
                temp_chunk = []

            if i == self.len_:
                assert len(temp_chunk) == self.overflow_len

                if self.overflow == 'first':
                    self.chunks[0] += temp_chunk
                else:
                    self.chunks[-1] += temp_chunk

        return self.chunks

    def get_chunks_gen(self):

        temp_chunk = []
        for i, elem in enumerate(self.data, start=1):

            temp_chunk.append(elem)
            if not i % self.each_chunk and self.chunk_count < self.num_chunks:
                # self.chunks.append(temp_chunk)
                if self.chunk_count != self.num_chunks - 1:
                    self.chunk_count += 1
                    yield temp_chunk
                    temp_chunk = []

            if i == self.len_:
                assert len(temp_chunk) == self.overflow_len + self.each_chunk
                yield temp_chunk
