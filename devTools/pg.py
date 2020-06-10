import concurrent.futures
import multiprocessing


def gen(n):
    while n > 0:
        yield n
        n -= 1


def sq(x):
    return x**2


def does_nothing():
    print('YAY')


class Worker(object):
    """Worker class for running the job"""
    def __init__(self, id, job):
        """
        Constructor for Worker class
        Takes in:
        ID -> Number
        job -> a function
        type -> 'MAP' or 'REDUCE'
        """

        super(Worker, self).__init__()

        self.type_dict = {0: 'MAP', 1: 'REDUCE'}
        self.job = job  # Function
        self.id = id
        self.type = type

    def set_map_data(self, data):
        """
        To set data before parallel execution
        """

        self.data = data

    def run(self):

        print(f'{self.type} Worker {self.id} is working!!', end='\r')

        if self.type == 'MAP':
            result = self.job(self.data)
        else:
            result = self.job(A, B)

    def __str__(self):
        return f'<Worker - {self.type} - {self.id} at {id(self)}>'

    def __repr__(self):
        return str(self)


def main():

    iter_ = gen(10)

    cpus = multiprocessing.cpu_count()
    print(cpus)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(does_nothing) for _ in range(5)]

    for future in futures:
        print(future.result())


if __name__ == '__main__':
    main()
