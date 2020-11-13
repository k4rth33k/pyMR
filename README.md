# pyParallelMR

## Simple Map Reduce package in python that supports parallel processing

![Python package](https://github.com/k4rth33k/pyMR/workflows/Python%20package/badge.svg?branch=master) ![enter image description here](https://raw.githubusercontent.com/k4rth33k/pyMR/master/badges/pip-install.svg) ![enter image description here](https://raw.githubusercontent.com/k4rth33k/pyMR/master/badges/python_ver.svg) ![enter image description here](https://raw.githubusercontent.com/k4rth33k/pyMR/master/badges/version.svg)
  
![Experiment results](https://k4rth33k.github.io/files/PrimesPlain.png)
## In a nutshell
 - pyParallelMR leverages the fact that greater speeds in computation can be achieved using more memory. 
 - It is not applicable to problems which require optimized solutions (IoT for example).
 - A simple queue based map reduce engine which executes tasks in a batch wise parallel manner. (forcible fanciness :wink:) 

## Usage
```python
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
```

## Results

 - Finding primes in a given range
 - Word count of a document
 
 ### Finding primes in a given range
 ![enter image description here](https://k4rth33k.github.io/files/PrimesTradeoff.png)
The objective of this painstaking ordeal of heavy multiprocessing and a bit of bad python code (sarcasm :stuck_out_tongue_closed_eyes:) is to achieve the high speeds with respect to CPUs completely ignoring the memory usage.

# Other results will be added soon and thoughts and contributions are always welcome :purple_heart:
