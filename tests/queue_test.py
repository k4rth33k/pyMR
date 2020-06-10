from pyMR import Queue


def test_enq():
    q = Queue()
    assert q.is_empty() == True

    for i in range(10):
        q.enqueue(i)

    for i in range(10):
        assert q.dequeue() == i


def test_deq():
    q = Queue()
    try:
        q.dequeue()
    except:
        pass


def test_size():
    q = Queue()
    for i in range(1, 10):
        q.enqueue(i)
        q.dequeue()

    assert q.size() == 0
