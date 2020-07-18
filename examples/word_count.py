# root/examples/word_count.py

from pyMR import Master
from collections import defaultdict


# Map logic for word count
def map_wc(lines):
    word_count = defaultdict(int)

    for line in lines:
        for word in line.split():
            word_count[word] += 1

    return word_count


# Reduce logic for word count
def red_wc(kv1, kv2):
    for word, count in kv1.items():
        kv2[word] += count
    return kv2


def main():
	file_path = '/path/to/file'
    master = Master(num_workers=9) # Initialize engine
    master.create_job(data=open(file_path, encoding='utf-8'),
                      map_fn=map_wc, red_fn=red_wc) # Submit the data
    master.start() # Run the job

    result =  master.result()
    print(result)


if __name__ == '__main__':
    main()
