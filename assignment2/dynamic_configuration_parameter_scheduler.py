#!/home/cc-group2/miniconda3/bin/python3

from datetime import datetime
import subprocess
import random
import math
import sys
import csv
import os

NAMES = ['executorAllocationRatio', 'batchDelay', 'initialExecutors', 'defaultParallelism', 'batchSize', 'schedulerBacklogTimeout']

RANGES = {
	'executorAllocationRatio': (0.75, 1.0),
	'batchDelay': (500., 7000.0),
	'initialExecutors': (1.0, 11.0),
	'defaultParallelism': (1.0, 21.0),
	'batchSize': (1.0, 21.0),
	'schedulerBacklogTimeout': (1.0, 21.0)
}

ISFLOAT = {
	'executorAllocationRatio': True,
	'batchDelay': False,
	'initialExecutors': False,
	'defaultParallelism': False,
	'batchSize': False,
	'schedulerBacklogTimeout': False
}

UNITS = {
	'executorAllocationRatio': "",
	'batchDelay': "ms",
	'initialExecutors': "",
	'defaultParallelism': "",
	'batchSize': "",
	'schedulerBacklogTimeout': "s"
}

PROP = {
	'executorAllocationRatio': "--conf spark.dynamicAllocation.executorAllocationRatio=",
	'batchDelay': "--conf spark.kubernetes.allocation.batch.delay=",
	'initialExecutors': "--conf spark.dynamicAllocation.initialExecutors=",
	'defaultParallelism': "--conf spark.default.parallelism=",
	'batchSize': "--conf spark.kubernetes.allocation.batch.size=",
	'schedulerBacklogTimeout': "--conf spark.dynamicAllocation.schedulerBacklogTimeout="
}

INITIAL_STEP_SIZE = 0.02
LEARNING_RATE_CONSTANT = 0.05

def randomRange(r):
	return (random.random() * (r[1] - r[0])) + r[0]

def makeStep(start, step, toTest, ranges):
	res = {}

	for param in toTest:
		res[param] = start[param] + step[param]
		lo = ranges[param][0]
		hi = ranges[param][1]
		while res[param] < lo or res[param] > hi:
			if res[param] < lo:
				res[param] = lo + (lo - res[param])
			elif res[param] > hi:
				res[param] = hi - (res[param] - hi)
	
	return res

def randomVector(length, toTest, ranges):
	vec = {}
	mag = 0

	for param in toTest:
		r = random.gauss(0, 1)
		vec[param] = r
		mag += r**2

	mag = mag ** 0.5

	for param in toTest:
		vec[param] *= length * (ranges[param][1] - ranges[param][0]) / mag

	return vec

# use history to calculate the new params
def updateParams(h, toTest, toIgnore, ranges, learningRate):
	if len(h) == 0:
		# base case - generate random parameter
		params = {}
		for param in toTest:
			params[param] = randomRange(ranges[param])
		# should not set ignored params but let them be defaulted
		# for param in toIgnore:
		# 	params[param] = ranges[param][0]
		
		return params
	elif len(h) == 1:
		# base case - take a random step
		return makeStep(h[0][0], randomVector(INITIAL_STEP_SIZE, toTest, ranges), toTest, ranges)
	else:
		# take step based on previous
		deltaRes = h[-1][1] - h[-2][1]
		step = {}

		for param in toTest:
			delta = (h[-1][0][param] - h[-2][0][param]) / (ranges[param][1] - ranges[param][0])
			step[param] = -deltaRes * learningRate * delta
		
		return makeStep(h[-1][0], step, toTest, ranges)

def getParam(params, name):
	x = params[name]
	x = x if ISFLOAT[name] else math.floor(x)
	x = str(x) + UNITS[name]
	return x

def getConfig(params, names):
	conf = ""
	for name in names:
		conf += PROP[name] + getParam(params, name) + " "
	return conf


def chooseParams(options, ranges):
	# sets everything to the lower bound
	baseLineParams = {}
	for param in options:
		baseLineParams[param] = ranges[param][0]
	baseLineTime = runOnCluster(baseLineParams, options)

	history = [(baseLineParams, baseLineTime)]
	diff = {}
	for param in options:
		newParams = { x: baseLineParams[x] for x in baseLineParams }
		newParams[param] = ranges[param][1]
		diff[param] = runOnCluster(baseLineParams, options)
		history.append((newParams, diff[param]))
	
	sortParams = [p for p,v in sorted(diff.items(), key=lambda item: abs(item[1]-baseLineTime), reverse=True)]

	# toTest, toIgnore, history
	return (sortParams[:3], sortParams[3:], history)

def runOnCluster(params, names):
	# return random.uniform(0,1)
	out = subprocess.run(('/usr/bin/time --format %e ./spark-3.3.0-bin-hadoop3/bin/spark-submit \
					--master k8s://https://128.232.80.18:6443 \
					--deploy-mode cluster \
					--name group2-wordcount \
					--class com.cloudcomputing.group2.assignment1.App \
					--conf spark.kubernetes.namespace=cc-group2 \
					--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
					--conf spark.kubernetes.container.image=andylamp/spark:v3.3.0 \
					--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
					--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
					--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
					--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
					--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
					--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
					--conf spark.dynamicAllocation.enabled=true \
					--conf spark.dynamicAllocation.shuffleTracking.enabled=true ' + getConfig(params, names) + ' \
					local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/data-200MB.txt').split(),capture_output=True).stderr
	os.system('kubectl get pods | grep driver | awk \'{ print "kubectl delete pod "$1 }\' | bash')
	return float(out.decode('utf-8').strip())

def optimise(toTest, toIgnore, iter):
	params = updateParams([], toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT)
	history = []

	for i in range(1, iter + 1):
		print("trying params: " + str(params))
		
		t = runOnCluster(params, toTest)
		print("t: " + str(t))

		# create new timestamp folder

		#update history
		history.append((params, t))

		# update parameters
		params = updateParams(history, toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT / i)
	return history

today = datetime.now()
iso = today.isoformat().replace("-", "").replace(":", "").replace(".", "")[:-6]

print("running")
toTest, toIgnore, history = chooseParams(NAMES, RANGES)
print("selecting params: "+', '.join(toTest))
print("ignoring param: "+', '.join(toIgnore))
history += optimise(toTest, toIgnore, 13)

if not os.path.isdir('./experiments'):
    os.mkdir('./experiments')

if os.path.isdir(f'./experiments/{iso}'):
    print('ERROR: folder with same timestamp already exists')
    sys.exit(1)

os.mkdir(f'./experiments/{iso}')

# write history to file
with open('./experiments/' + iso + '/dynamic.csv', 'w', newline='') as f:
	writer = csv.writer(f)
	print(", ".join(toTest + toIgnore), 'Execution Time')
	writer.writerow(toTest + toIgnore + ['Execution Time'])
	for i in range(0, len(history)):
		row = []
		for name in toTest:
			row.append(history[i][0].get(name))
		for name in toIgnore:
			row.append(history[i][0].get(name,""))
		row.append(history[i][1])
		writer.writerow(row)
		print(row)
