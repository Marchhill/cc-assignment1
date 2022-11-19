import subprocess
import random

RANGES = ((0.0, 1.0), (0.0, 1.0), (0.0, 1.0))
INITIAL_STEP_SIZE = 0.001
LEARNING_RATE = 0.01

def randomRange(r):
	return (random.random() * (r[1] - r[0])) + r[0]

def makeStep(start, step, ranges):
	res = [0.0, 0.0, 0.0]

	for i in range(0, 3):
		res[i] = start[i] + step[i]
		if res[i] < ranges[i][0]:
			res[i] = ranges[i][0] + (ranges[i][0] - res[i])
		elif res[i]> ranges[i][1]:
			res[i] = ranges[i][1] - (res[i] - ranges[i][1])
	
	return tuple(res)

def randomVector(length):
	vec = [random.gauss(0, 1) for i in range(3)]
	mag = sum(x**2 for x in vec) ** 0.5
	return tuple([x * length / mag for x in vec])

# use history to calculate the new params
def updateParams(h):
	if len(h) == 0:
		# base case - generate random parameter
		# todo: fix to be
		return (randomRange(RANGES[0]), randomRange(RANGES[1]), randomRange(RANGES[2]))
	elif len(h) == 1:
		# base case - take a random step
		return makeStep(h[0][0], randomVector(INITIAL_STEP_SIZE), RANGES)
	else:
		# take step based on previous
		deltaRes = h[-1][1] - h[-2][1]
		step = [0.0, 0.0, 0.0]

		for i in range(0, 3):
			delta = h[-1][0][i] - h[-2][0][i]
			step[i] = -deltaRes * LEARNING_RATE / delta
		
		return makeStep(h[-1][0], tuple(step), RANGES)

params = updateParams([])
history = []

for i in range(0, 20):
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
				local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/data200MBtxt').split(),capture_output=True).stderr
	t = float(out.decode('utf-8').strip())
	'''
	t = params[0]*params[0] + params[1]*params[1] + params[2]*params[2]

	# create new timestamp folder

	#update history
	history.append((params, t))

	# update parameters
	params = updateParams(history)

# write history to file
for i in range(0, len(history)):
	print("params: " + str(history[i][0]) + " -> " + str(history[i][1]))