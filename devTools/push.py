# root/scripts/clear.py

import os
import clear
import format

os.system('pip install . --no-cache-dir')

cmt = input('Commit message: ')

os.system('git add .')

os.system(f'git commit -m \"{cmt}\"')

branch = input('Branch: ')
os.system(f'git push origin {branch}')
