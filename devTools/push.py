# root/scripts/clear.py

import os
import clear
import format

cmt = input('Commit message: ')


os.system('git add .')

os.system(f'git commit -m "{cmt}"')

os.system('git push origin master')