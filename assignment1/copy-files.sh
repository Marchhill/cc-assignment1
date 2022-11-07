#!/bin/bash

scp -i ~/.ssh/cloud *.py cc-group2@caelum-109.cl.cam.ac.uk:~
# ssh -i ~/.ssh/cloud cc-group2@caelum-109.cl.cam.ac.uk "./run-and-plot.py"