#!/usr/bin/python3

from datetime import datetime
import os
import sys

today = datetime.now()
# strip away separators and milliseconds
iso = today.isoformat().replace("-", "").replace(":", "").replace(".", "")[:-6]

if os.path.isdir(f'./experiments/{iso}'):
    print('ERROR: folder with same timestamp already exists')
    sys.exit(1)

os.mkdir(f'./experiments/{iso}')

os.system(f'./run-experiments.py {iso}')
os.system(f'./plot-experiments.py {iso}')
