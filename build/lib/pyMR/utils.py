# root/pyMR/utils.py


class Stack(object):
    def __init__(self, master):
        super(Stack, self).__init__()
        self._ = []

    def push(self, elem):
        self._.append(elem)
        if len(self._) == 2:
            master.reduce(self.pop(), self.pop())

    def pop(self):
        return self._.pop()
