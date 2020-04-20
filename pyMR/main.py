# root/pyMR/main.py


class Master(object):
    """Master class for creating and running workers"""
    def __init__(self, name):
        super(Master, self).__init__()
        self.name = name


class Worker(object):
    """Worker class for running the job"""
    def __init__(self):
        super(Worker, self).__init__()
        pass
