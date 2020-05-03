from pyMR import Chunks


def test_chunk_list():
    list_ = [i for i in range(1, 103)]
    chunks_first = Chunks(list_, 5, overflow='first').get_chunks()
    chunks_last = Chunks(list_, 5, overflow='last').get_chunks()

    assert ([len(_) for _ in chunks_last] == [20, 20, 20, 20, 22])
    assert ([len(_) for _ in chunks_first] == [22, 20, 20, 20, 20])


def test_chunk_str():
    str_ = ''.join(str(x) for x in range(1, 1000))

    chunks_first = Chunks(str_, 20, overflow='first').get_chunks()
    chunks_last = Chunks(str_, 20, overflow='last').get_chunks()


def test_chunk_file():
    # Number of lines in file is 1281
    # num_chunks = 200 => overflow = 81 => each_chunk = 6
    with open('./tests/test_files/test.txt', 'r') as fp:
        chunks_first = Chunks(fp.readlines(), 200,
                              overflow='first').get_chunks()

    with open('./tests/test_files/test.txt', 'r') as fp:
        chunks_last = Chunks(fp.readlines(), 200).get_chunks()

    assert len(chunks_first) == 200
    assert len(chunks_last[-1]) == 87
    assert len(chunks_first[0]) == 87


def test_type_check():
    try:
        chunks_1 = Chunks(5, 5).get_chunks()
    except Exception as e:
        print(e)

    try:
        chunks_2 = Chunks([1, 2, 3, 4, 5, 6, 7, 8, 9], 10).get_chunks()
    except Exception as e:
        print(e)


def test_gen():
    x = [_ for _ in range(1000)]
    chunks = Chunks(x, 10)

    gen = chunks.get_chunks_gen()

    assert len(next(gen)) == 100
