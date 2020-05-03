# root/test/simple_test.py

from pyMR import Master


def map_(x):
    print('Running map')
    return sum(x)


def red_(x, y):
    return x + y

# 1000000000
def test_main():
    master = Master(num_workers=30)
    # master.create_job(data=range(100),
    #                   map_fn=lambda x: sum(x),
    #                   red_fn=lambda x, y: x + y)
    master.create_job(data=list(range(1000000000)),
                      map_fn=map_,
                      red_fn=red_)

    master.start()
    result = master.result()
    print(result)


# def test_len():
#     master = Master(num_workers=5)
#     master.create_job(data=range(101),
#                       map_fn=lambda x: sum(x),
#                       red_fn=lambda x, y: x + y)

#     assert len(master.workers) == 5

if __name__ == '__main__':
    test_main()
