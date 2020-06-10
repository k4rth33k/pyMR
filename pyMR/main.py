# root/pyMR/main.py

from .chunk import Chunks
import concurrent.futures
from .utils import Queue


class ArgumentError(Exception):
    pass


class Master(object):
    """Master class for creating and running workers"""
    def __init__(self,
                 num_workers,
                 split_ratio=0.75,
                 granularity=3,
                 verbose=True):
        """
        Constructor for Master, master orchestrates the execution of workers

        Args:
            num_workers (int): Number of workers needed to be spawn
            split_ratio (float): Ratio of map to reduce workers
            granularity (int): # Chunks needed by each map worker
            verbose (bool): set to True to print logss
        Returns:
            Nothing
        Raises:
            ArgumentError: if arguments are not of right type
        """
        super(Master, self).__init__()

        self.__workers = []

        self.num_workers = num_workers
        self.verbose = verbose
        self.granularity = granularity
        self.__map_num = int(split_ratio * num_workers)
        self.__red_num = self.num_workers - self.__map_num
        self.queue = Queue()

        self.validate = None

    def create_job(self, data, map_fn, red_fn):
        """
        Initializes chunks and map/reduce functions

        Args:
            data (Iterable): Data to be processed(Generator is highly recommended)
            map_fn (Function): Function (not lambda) to be applied on chunks
            red_fn (Function): Function (not lambda) to be applied on results
        Returns:
        Raises:
        """

        self.map_fn = map_fn
        self.red_fn = red_fn

        chunks = Chunks(data=data,
                        num_chunks=self.__map_num * self.granularity)
        self.data_gen = chunks.get_chunks_gen()

    def start(self):
        """
        Starts mapReduce execition loop, refer[link]

        Args:
        Returns:
        Raises:
        """

        print('Job has started!' * self.verbose, end='\n' * self.verbose)

        data_empty = False
        converted = False

        # Initialize worker objects
        self.__workers = [
            Worker(id=_, job=self.map_fn, type='MAP', verbose=self.verbose)
            for _ in range(1, self.__map_num + 1)
        ]
        self.__workers += [
            Worker(id=_, job=self.red_fn, type='RED', verbose=self.verbose)
            for _ in range(self.__map_num + 1, self.__red_num + 1)
        ]

        while not self.__verify():
            count = 0

            if not data_empty:
                # Setting up map workers
                for worker in self.__workers[:self.__map_num]:
                    try:
                        worker.set_data(next(self.data_gen))
                        count += 1
                    except StopIteration:
                        data_empty = True

            if data_empty and not converted:
                for worker in self.__workers[:self.__map_num]:
                    worker.type = 'RED'
                    worker.job = self.red_fn
                converted = True

            # Setting up reduce workers
            for worker in self.__workers:
                if self.queue.size() >= 2 and worker.type == 'RED':
                    count += 1
                    A, B = self.queue.dequeue(), self.queue.dequeue()
                    worker.set_data([A, B])

            with concurrent.futures.ProcessPoolExecutor() as executor:
                results = [
                    executor.submit(worker.run)
                    for worker in self.__workers[:count]
                ]

            for f in concurrent.futures.as_completed(results):
                f.add_done_callback(self.__take_result)

    def __take_result(self, future):
        result = future.result()
        self.queue.enqueue(result)

    def __verify(self):
        if self.queue.size() == 1:
            if self.validate:
                return id(self.validate) == id(self.queue.peek())
            else:
                self.validate = self.queue.peek()

        return False

    def result(self):
        return self.validate

    def __str__(self):
        return '<Master obj at {}>'.format(id(self))

    def __repr__(self):
        return str(self)


class Worker(object):
    """Worker class for running the job"""
    def __init__(self, id, job, type, verbose):
        """
        Constructor for Worker class
        Takes in:
        ID -> Number
        job -> a function
        type -> 'MAP' or 'REDUCE'
        """

        super(Worker, self).__init__()
        self.job = job  # Function
        self.id = id
        self.type = type
        self.verbose = verbose

    def set_data(self, data):
        """
        To set data before parallel execution
        """
        self.data = data

    def run(self):

        print(('{} Worker {} is working'.format(self.type, self.id)) *
              self.verbose,
              end='\r' * self.verbose)

        if self.type == 'MAP':
            result = self.job(self.data)
        else:
            result = self.job(self.data.pop(), self.data.pop())

        return result
