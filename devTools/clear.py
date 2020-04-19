# root/scripts/clear.py

import os
import pathlib

_ = [p.unlink() for p in pathlib.Path('.').rglob('*.py[co]')]
_ = [p.rmdir() for p in pathlib.Path('.').rglob('__pycache__')]

for root, subdirs, files in os.walk('.'):
    for file in files:
        if '.py' in file:
            _ = [p.unlink() for p in pathlib.Path(root).rglob('*.py[co]')]

print('Clearning cache')
