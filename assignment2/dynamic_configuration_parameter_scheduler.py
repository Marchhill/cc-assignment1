import subprocess
import random
import math

# params: executorAllocationRation, batch.delay, initialExecutors

RANGES = {
	'executorAllocationRation': (0.75, 1.0),
	'batch.delay': (500., 7000.0),
	'initialExecutors': (1.0, 11.0)
}

ISFLOAT = {
	'executorAllocationRation': True,
	'batch.delay': False,
	'initialExecutors': False
}

INITIAL_STEP_SIZE = 0.01
LEARNING_RATE_CONSTANT = 0.02

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
			params[param] = randomRange(ranges(param))
		for param in toIgnore:
			params[param] = ranges[param][0]
		
		return params
	elif len(h) == 1:
		# base case - take a random step
		return makeStep(h[0][0], toTest, randomVector(INITIAL_STEP_SIZE, toTest, ranges), ranges)
	else:
		# take step based on previous
		deltaRes = h[-1][1] - h[-2][1]
		step = {}

		for param in toTest:
			delta = h[-1][0][param] - h[-2][0][param]
			step[param] = -deltaRes * learningRate * (ranges[param][1] - ranges[param][0]) / delta
		
		return makeStep(h[-1][0], step, ranges)

def getParam(params, name):
	x = params[name]
	x = x if ISFLOAT[name] else math.floor(x)
	return str(x)

def chooseParams(options):
	# toTest, toIgnore, history
	return ([], [], [])

def optimise(toTest, toIgnore, iter):
	params = updateParams([], toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT)
	history = []

	for i in range(1, iter + 1):
		print("trying params: " + str(params))
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
					--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
					--conf spark.dynamicAllocation.executorAllocationRatio=' + getParam(params, 'executorAllocationRation') + ' \
					--conf spark.kubernetes.allocation.batch.delay=' + getParam(params, 'batch.delay') + 'ms \
					--conf spark.dynamicAllocation.initialExecutors=' + getParam(params, 'initialExecutors') + ' \
					local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/data-200MB.txt').split(),capture_output=True).stderr
		
		t = float(out.decode('utf-8').strip())

		print("t: " + str(t))

		# create new timestamp folder

		#update history
		history.append((params, t))

		# update parameters
		params = updateParams(history, toTest, toIgnore, RANGES, LEARNING_RATE_CONSTANT / i)

toTest, toIgnore, history = chooseParams(['executorAllocationRation', 'batch.delay', 'initialExecutors'])
history += optimise(toTest, toIgnore, 13)

# write history to file
for i in range(0, len(history)):
	print("params: " + str(history[i][0]) + " -> " + str(history[i][1]))