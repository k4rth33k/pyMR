from pyMR import Master
from math import sqrt
import tracemalloc
import datetime
import pandas as pd
import tracemalloc


def time_this(target):
    def inner(*args):
        tracemalloc.start()
        start = datetime.datetime.now()
        target(*args)
        end = datetime.datetime.now()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        delta = end - start
        return delta.total_seconds(), peak / 10**6

    return inner


def map_prime(list_):
    primes = []

    for val in list_:
        flag = True
        for i in range(2, int(sqrt(val))):
            if val % i == 0:
                flag = False
                break
        if flag:
            primes.append(val)

    return primes


def red_prime(primes1, primes2):
    return primes1 + primes2


@time_this
def with_pymr(high_bound, x):
    master = Master(num_workers=9 + x, granularity=3 * x, verbose=False)
    master.create_job(data=range(high_bound),
                      map_fn=map_prime,
                      red_fn=red_prime)
    master.start()


@time_this
def without_pymr(high_bound):
    map_prime(range(high_bound))


def main():
    df = pd.DataFrame(columns=[
        'Range', 'Raw (sec)', 'pyMR (sec)', 'Raw peek (MB)', 'pyMR peek (MB)'
    ])

    for i in range(2, 10):
        try:
            print(f'Finding prime numbers in range - {10 ** i}')
            with_time, with_peek = with_pymr(10**i, i)
            without_time, without_peek = without_pymr(10**i)
            df_ = pd.DataFrame(
                [[10**i, without_time, with_time, without_peek, with_peek]],
                columns=[
                    'Range', 'Raw (sec)', 'pyMR (sec)', 'Raw peek (MB)',
                    'pyMR peek (MB)'
                ])
            df = df.append(df_)
        except KeyboardInterrupt:
            break

    print('-' * 50)
    print(df)
    print('-' * 50)
    df.to_csv('Prime Benchmark.csv')


if __name__ == '__main__':
    main()
