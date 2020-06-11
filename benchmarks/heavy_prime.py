from pyMR import Master
from math import sqrt
import tracemalloc
import time
import datetime
import pandas as pd


def time_this(target):
    def inner(x):
        start = datetime.datetime.now()
        target(x)
        end = datetime.datetime.now()
        delta = end - start
        return delta.total_seconds()

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
def with_pymr(high_bound):
    master = Master(10, 3, verbose=False)
    master.create_job(data=range(high_bound),
                      map_fn=map_prime,
                      red_fn=red_prime)
    master.start()


@time_this
def without_pymr(high_bound):
    map_prime(range(high_bound))


def main():
    df = pd.DataFrame(columns=['Range', 'Without pyMR', 'With pyMR'],
                      index=False)

    for i in range(2, 10):
        print(f'Finding prime numbers in range - {10 ** i}')
        with_time = with_pymr(10**i)
        without_time = without_pymr(10**i)
        df_ = pd.DataFrame([[10**i, without_time, with_time]],
                           columns=['Range', 'Without pyMR', 'With pyMR'],
                           index=False)
        df = df.append(df_)

    print('-' * 50)
    print(df)
    print('-' * 50)


if __name__ == '__main__':
    main()
