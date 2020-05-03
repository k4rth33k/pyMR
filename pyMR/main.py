# root/pyMR/main.py

from .chunk import Chunks2
from .chunk import Chunks
import concurrent.futures
from .utils import Stack


class Master(object):
    """Master class for creating and running workers"""
    def __init__(self, num_workers, split_ratio=0.8):
        super(Master, self).__init__()

        self.data_gen = None
        self.num_workers = num_workers
        self.workers = []

        self.map_results = []
        self.reduce_stack = Stack(self)

        self.map_workers = int(split_ratio * num_workers)
        self.reduce_workers = num_workers - self.map_workers

    def create_job(self, data, map_fn, red_fn):

        self.map_fn = map_fn
        self.red_fn = red_fn

        self.data_gen = Chunks(data=data,
                               num_chunks=self.num_workers).get_chunks_gen()

        # self.data_gen = iter(Chunks2(data=data,
        # num_chunks=self.map_workers))

        # self.workers = [Worker(id=_, data_gen=self.data_gen,
        #                        job=map_fn, type='MAP',
        # master=self) for _ in range(1, self.map_workers + 1)]

        # self.workers += [Worker(id=_, data_gen=self.data_gen,
        #                         job=red_fn, type='REDUCE',
        #                         master=self) for _ in range(self.map_workers + 1,
        # self.num_workers + 1)]

        # Priming
        # self.workers = [Worker(id=_, data=next(self.data_gen),
        # job=map_fn, type='MAP') for _ in range(1, self.num_workers)]

    def start(self):
        print('Job has started!')
        processes = []

        # while (generator is not empty):
        self.workers = [
            Worker(id=_, data=next(self.data_gen), job=self.map_fn, type='MAP')
            for _ in range(1, self.num_workers)
        ]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [
                executor.submit(worker.run)
                for worker in self.workers[:self.map_workers]
            ]

        # self.map_results = [f.result()
        #                for f in concurrent.futures.as_completed(results)]
        # self.map_results = [f.add_done_callback(self.take_result)
        # for f in concurrent.futures.as_completed(results)]

        for f in concurrent.futures.as_completed(results):
            f.add_done_callback(self.take_result)

        print()

    def take_result(self, future):
        result = future.result()
        self.map_results.append(result)

        # print('\n', result)

    def result(self):
        return self.map_results

    def __str__(self):
        return f'<Master obj at {id(self)}>'

    def __repr__(self):
        return str(self)


# class Master(object):
#     """Master class for creating and running workers"""

#     def __init__(self, num_workers):
#         super(Master, self).__init__()

#         self.data_gen = None
#         self.num_workers = num_workers
#         self.workers = []

#         self.map_results = []
#         self.reduce_stack = Stack(self)

#     def create_job(self, data, map_fn, red_fn):

#         self.data = Chunks2(data)
#         self.map_fn = map_fn
#         self.red_fn = red_fn

#     def start(self):
#         print('Job has started!')
#         processes = []

#         with concurrent.futures.ProcessPoolExecutor() as executor:
#             results = executor.map(self.map_fn, self.data)

#         # self.map_results = [f.result()
#         #                for f in concurrent.futures.as_completed(results)]
#         # self.map_results = [f.add_done_callback(self.take_result)
#         # for f in concurrent.futures.as_completed(results)]

#         self.results = [result for result in results]
#         # print(self.results)

#     def take_result(self, future):
#         result = future.result()
#         print('\n', result)

#     def result(self):
#         return self.results

#     def __str__(self):
#         return f'<Master obj at {id(self)}>'

#     def __repr__(self):
#         return str(self)


class Worker(object):
    """Worker class for running the job"""
    def __init__(self, id, data, job, type):

        super(Worker, self).__init__()
        self.job = job  # Function
        # self.data_gen = data_gen
        self.data = data
        self.id = id
        self.type = type
        self.result = None
        self.working = False

    def run(self, A=None, B=None):

        print(f'{self.type} Worker {self.id} is working!!', end='\r')
        self.working = True
        # data = None

        if self.type == 'MAP':
            # try:
            #     while data is None:
            #         data = next(self.data_gen)
            #         print(self.id, data)

            # except Exception as e:
            #     print(e)

            # self.result = self.job(data)
            self.result = self.job(self.data)

            # return self.result
        else:
            self.result = self.job(A, B)

        self.working = False
        return self.result

    def __str__(self):
        return f'<Worker - {self.type} - {self.id} at {id(self)}>'

    def __repr__(self):
        return str(self)
