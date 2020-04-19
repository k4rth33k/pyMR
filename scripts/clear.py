# root/scripts/clear.py

import os

os.system("python3 -Bc \"import pathlib; [p.unlink() for p in pathlib.Path('.').rglob('*.py[co]')]\"")
os.system("python3 -Bc \"import pathlib; [p.rmdir() for p in pathlib.Path('.').rglob('__pycache__')]\"")

