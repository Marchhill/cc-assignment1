#!/home/cc-group2/miniconda3/bin/python3

from datetime import datetime
import subprocess
import random
import math
import sys
import csv
import os

FILE = ""

NAMES = ['executorAllocationRatio', 'batchDelay', 'initialExecutors', 'defaultParallelism', 'batchSize', 'schedulerBacklogTimeout']

RANGES = {
	'executorAllocationRatio': (0.75, 1.0),
	'batchDelay': (500., 7000.0),
	'initialExecutors': (1.0, 10.5),
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

INITIAL_STEP_SIZE = 0.05
LEARNING_RATE_CONSTANT = 0.05

def sigmoid(x):
	return 1 / (1 + math.e ** (-x))

def sigmoid_inv(y):
	return -math.log(1/y - 1)

REPULSION = 0.5
def repulse(val, low, high):
	# val=high is mapped to x=1/REPULSION
	# val=low is mapped to x=-1/REPULSION
	# sigmoid=1 is mapped to high
	# sigmoid=0 is mapped to low
	repulseBound = 1/REPULSION
	adjVal = (val - low) / (high - low) * 2 * repulseBound - repulseBound
	return sigmoid(adjVal) * (high - low) + low

def repulse_inv(val, low, high):
	repulseBound = 1/REPULSION
	adjVal = sigmoid_inv((val-low)/(high-low))
	return (adjVal + repulseBound) * (high - low) / (2 * repulseBound) + low

def randomRange(r):
	# selects a random starting point within the specified range
	return (random.random() * (r[1] - r[0])) + r[0]

def makeStep(start, step, toTest):
	res = {}

	for param in toTest:
		res[param] = start[param] + step[param]
	
	return res

def changeVector(length, toTest, ranges, changes):
	vec = {}
	mag = 0

	for param in toTest:
		r = changes[param] * abs(random.gauss(0, 1))
		vec[param] = r
		mag += r**2

	mag = mag ** 0.5

	for param in toTest:
		vec[param] *= length * (ranges[param][1] - ranges[param][0]) / mag

	return vec

# use history to calculate the new params
def updateParams(h, toTest, toIgnore, ranges, learningRate, changes):
	if len(h) == 0:
		# base case - generate random parameter
		params = {}
		for param in toTest:
			params[param] = randomRange(ranges[param])
		# should not set ignored params but let them be defaulted
		
		return params
	elif len(h) == 1:
		# take a step towards the direction that reduces the time
		return makeStep(h[0][0], changeVector(INITIAL_STEP_SIZE, toTest, ranges, changes), toTest)
	else:
		# take step based on previous
		deltaRes = h[-1][1] - h[-2][1]
		step = {}

		for param in toTest:
			lo = ranges[param][0]
			hi = ranges[param][1]
			delta = (h[-1][0][param] - h[-2][0][param]) / (ranges[param][1] - ranges[param][0])
			step[param] = -deltaRes * learningRate / delta
		
		return makeStep(h[-1][0], step, toTest)

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
	# return (options[:3], options[3:], [])
	# sets everything to the lower bound
	baseLineParams = {}
	for param in options:
		baseLineParams[param] = ranges[param][0]
	baseLineTime = runOnCluster(baseLineParams, options)

	history = [(baseLineParams, baseLineTime)]
	diff = {}
	change = {}
	# testing each param in turn by running it on the cluster, setting each to their upper bound
	for param in options:
		newParams = { x: baseLineParams[x] for x in baseLineParams }
		newParams[param] = ranges[param][1]
		diff[param] = runOnCluster(baseLineParams, options)
		history.append((newParams, diff[param]))
		change[param] = 1 if diff[param] - baseLineTime > 0 else -1
	
	sortParams = [p for p,v in sorted(diff.items(), key=lambda item: abs(item[1]-baseLineTime), reverse=True)]

	# toTest, toIgnore, history
	return (sortParams[:3], sortParams[3:], history, change)

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
					local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/'+FILE).split(),capture_output=True).stderr
	os.system('kubectl get pods | grep driver | awk \'{ print "kubectl delete pod "$1 }\' | bash')
	return float(out.decode('utf-8').strip())

def optimise(toTest, toIgnore, iter, changes):
	params = updateParams([], toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT, changes)
	history = []

	for i in range(1, iter + 1):
		print("trying params: " + str(params))
		boundParams = {k:repulse(v,*RANGES[k]) for k,v in params.items()}
		print("bound values: " + str(boundParams))
		
		t = runOnCluster(boundParams, toTest)
		print("t: " + str(t))

		# update history
		history.append((params, t))

		# update parameters
		params = updateParams(history, toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT / i, changes)
	# return a bounded parameter history
	return [({k:repulse(v,*RANGES[k]) for k,v in params.items()}, t) for params,t in history]

def main():
	today = datetime.now()
	iso = today.isoformat().replace("-", "").replace(":", "").replace(".", "")[:-6]

	print("running")
	toTest, toIgnore, history, change = chooseParams(NAMES, RANGES)
	print("selecting params: "+', '.join(toTest))
	print("ignoring param: "+', '.join(toIgnore))
	history += optimise(toTest, toIgnore, 13, change)

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
				row.append(history[i][0].get(name, ""))
			row.append(history[i][1])
			writer.writerow(row)
			print(row)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print('ERROR: folder with same timestamp already exists')
		sys.exit(1)
	
	FILE = sys.argv[1]
	main()