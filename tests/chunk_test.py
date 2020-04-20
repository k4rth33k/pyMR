from pyMR import get_chunks


def test_chunk_list():
    list_ = [i for i in range(1, 103)]
    chunks_first = get_chunks(list_, 5, overflow='first')
    chunks_last = get_chunks(list_, 5, overflow='last')

    assert ([len(_) for _ in chunks_last] == [20, 20, 20, 20, 22])
    assert ([len(_) for _ in chunks_first] == [22, 20, 20, 20, 20])


def test_chunk_str():
    str_ = ''.join(str(x) for x in range(1, 1000))

    chunks_first = get_chunks(str_, 20, overflow='first')
    chunks_last = get_chunks(str_, 20, overflow='last')


def test_chunk_file():
    # Number of lines in file is 1281
    # num_chunks = 200 => overflow = 81 => each_chunk = 6
    with open('./tests/test_files/test.txt', 'r') as fp:
        chunks_first = get_chunks(fp.readlines(), 200, overflow='first')

    with open('./tests/test_files/test.txt', 'r') as fp:
        chunks_last = get_chunks(fp.readlines(), 200)

    assert len(chunks_first) == 200
    assert len(chunks_last[-1]) == 87
    assert len(chunks_first[0]) == 87


def test_type_check():
    try:
        chunks_1 = get_chunks(5, 5)
    except Exception as e:
        print(e)

    try:
        chunks_2 = get_chunks([1, 2, 3, 4, 5, 6, 7, 8, 9], 10)
    except Exception as e:
        print(e)
