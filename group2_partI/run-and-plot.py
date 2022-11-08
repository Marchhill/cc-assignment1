#!/home/cc-group2/miniconda3/bin/python3

from datetime import datetime
import os
import sys

print("starting")
today = datetime.now()
# strip away separators and milliseconds
iso = today.isoformat().replace("-", "").replace(":", "").replace(".", "")[:-6]

# create the experiments folder if it doesn't exist
if not os.path.isdir('./experiments'):
    os.mkdir('./experiments')

if os.path.isdir(f'./experiments/{iso}'):
    print('ERROR: folder with same timestamp already exists')
    sys.exit(1)

os.mkdir(f'./experiments/{iso}')

os.system(f'python3 -u run-experiments.py {iso} && python3 -u plot-experiments.py {iso}')
