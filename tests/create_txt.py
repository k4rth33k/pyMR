with open('./tests/test_files/test.txt', 'w') as fp:
    for i in range(1281):
        fp.write(f'This is line {i}\n')
