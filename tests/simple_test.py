# root/test/simple_test.py

from pyMR import Master
from collections import defaultdict
from math import sqrt


def map_prime(x):
    temp = []
    for val in x:
        flag = False
        for i in range(2, int(sqrt(val)) + 1):
            if val % i == 0:
                flag = True
                break
        if not flag:
            temp.append(val)

    return temp


def map_sum(list_):
    return sum(list_)


def map_freq(lines):
    freq = defaultdict(int)
    for line in lines:
        for c in line:
            freq[c] += 1

    return freq


def red_list(x, y):
    return x + y


def red_freq(f1, f2):
    for letter, freq in f1.items():
        f2[letter] += freq

    return f2


# 1000000000


def test_prime():
    master = Master(num_workers=5, granularity=3, verbose=False)
    master.create_job(data=(range(1, 1000)), map_fn=map_prime, red_fn=red_list)

    master.start()
    result = master.result()

    assert set(result) == set(map_prime(range(1, 1000)))


def test_sum():
    master = Master(num_workers=5, granularity=3, verbose=False)
    master.create_job(data=(range(1, 1000)), map_fn=map_sum, red_fn=red_list)

    master.start()
    result = master.result()

    assert result == map_sum(range(1, 1000))


def test_freq():
    master = Master(num_workers=5, granularity=1, verbose=True)
    master.create_job(data=(open('./tests/test_files/test_small.txt', 'r')),
                      map_fn=map_freq,
                      red_fn=red_freq)

    master.start()
    result = master.result()

    assert result == map_freq(open('./tests/test_files/test_small.txt', 'r'))
