# root/pyMR/utils.py


class Stack(object):
    def __init__(self, master):
        super(Stack, self).__init__()
        self._ = []
        self.master = master

    def push(self, elem):
        self._.append(elem)
        if len(self._) == 2:
            master.reduce(self.pop(), self.pop())

    def pop(self):
        return self._.pop()


class Queue(object):
    """A simple queue to store results"""
    def __init__(self):
        super(Queue, self).__init__()
        self._ = []

    def enqueue(self, result):
        self._.append(result)

    def is_empty(self):
        return len(self._) == 0

    def dequeue(self):
        if self.is_empty():
            raise Exception('Cannot dequeue from an empty queue')
        x = self._[0]
        self._ = self._[1:]
        return x

    def size(self):
        return len(self._)

    def peek(self):
        return self._[0]

    def __str__(self):
        return str(self._)
