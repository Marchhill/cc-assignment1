import subprocess
import random
import math

# params: executorAllocationRation, batch.delay, initialExecutors

RANGES = ((0.75, 1.0), (1., 7.0), (1.0, 11.0))
INITIAL_STEP_SIZE = 0.01
LEARNING_RATE_CONSTANT = 0.02

def randomRange(r):
	return (random.random() * (r[1] - r[0])) + r[0]

def makeStep(start, step, ranges):
	res = [0.0, 0.0, 0.0]

	for i in range(0, 3):
		res[i] = start[i] + step[i]
		lo = ranges[i][0]
		hi = ranges[i][1]
		while res[i] < lo or res[i] > hi:
			if res[i] < lo:
				res[i] = lo + (lo - res[i])
			elif res[i] > hi:
				res[i] = hi - (res[i] - hi)
	
	return tuple(res)

def randomVector(length, ranges):
	vec = [random.gauss(0, 1) for i in range(3)]
	mag = sum(x**2 for x in vec) ** 0.5
	return tuple([vec[i] * length * (ranges[i][1] - ranges[i][0]) / mag for i in range(3)])

# use history to calculate the new params
def updateParams(h, ranges, learningRate):
	if len(h) == 0:
		# base case - generate random parameter
		# todo: fix to be
		return (randomRange(ranges[0]), randomRange(ranges[1]), randomRange(ranges[2]))
	elif len(h) == 1:
		# base case - take a random step
		return makeStep(h[0][0], randomVector(INITIAL_STEP_SIZE, ranges), ranges)
	else:
		# take step based on previous
		deltaRes = h[-1][1] - h[-2][1]
		step = [0.0, 0.0, 0.0]

		for i in range(0, 3):
			delta = h[-1][0][i] - h[-2][0][i]
			step[i] = -deltaRes * learningRate * (ranges[i][1] - ranges[i][0]) / delta
		
		return makeStep(h[-1][0], tuple(step), ranges)

params = updateParams([], RANGES, LEARNING_RATE_CONSTANT)
history = []

for i in range(1, 21):
	print("trying params: " + str(params))
	'''
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
				--conf spark.dynamicAllocation.executorAllocationRatio=' + str(params[0]) + ' \
				--conf spark.kubernetes.allocation.batch.delay=' + str(math.floor(params[1])) + 's \
				--conf spark.dynamicAllocation.initialExecutors=' + str(math.floor(params[2])) + ' \
				local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/data200MBtxt').split(),capture_output=True).stderr
	
	t = float(out.decode('utf-8').strip())
	'''
	t = params[0]**2 + params[1]**2 + params[2]**2
	print("t: " + str(t))

	# create new timestamp folder

	#update history
	history.append((params, t))

	# update parameters
	params = updateParams(history, RANGES, LEARNING_RATE_CONSTANT / i)

print("woo, finished!!")

# write history to file
for i in range(0, len(history)):
	print("params: " + str(history[i][0]) + " -> " + str(history[i][1]))