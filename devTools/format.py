# root/scripts/format.py

import os

for root, subdirs, files in os.walk('.'):
    for file in files:
        if '.py' in file:
            print(f'Formatting {root}/{file}')
            os.system(f'yapf -i {root}/{file}')
