#!/usr/bin/python3

import subprocess
import os
import time
import csv
import sys

# create measurements folder
if os.path.isdir('./experiments/' + sys.argv[1] + '/measurements'):
    print('ERROR: measurements folder with same timestamp already exists')
    sys.exit(1)

os.mkdir('./experiments/' + sys.argv[1] + '/measurements')

# carry out experiments
for file in ['data-100MB', 'data-200MB', 'data-500MB']:
	res = {'2': [], '4': [], '6': []}
	for i in range(0, 3):
		for executors in ['2', '4', '6']:
			out = subprocess.run(['ssh', '-i', '~/.ssh/cloud', 'cc-group2@caelum-109.cl.cam.ac.uk', '/usr/bin/time --format %e ./spark-3.3.0-bin-hadoop3/bin/spark-submit \
				--master k8s://https://128.232.80.18:6443 \
				--deploy-mode cluster \
				--name group2-wordcount \
				--class com.cloudcomputing.group2.assignment1.App \
				--conf spark.executor.instances=' + executors + ' \
				--conf spark.kubernetes.namespace=cc-group2 \
				--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
				--conf spark.kubernetes.container.image=andylamp/spark:v3.3.0 \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
				local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/' + file + '.txt'],capture_output=True).stderr
			seconds = float(out.decode('utf-8').strip())
			print(file + " with " + executors + " executors took " + str(seconds) + "s")
			res[executors].append(seconds)
		
		# wait 15 mins before starting next iteration
		time.sleep(15 * 60)
	
	# write to output file
	with open('./experiments/' + sys.argv[1] + '/measurements/' + file + '.csv', 'w', newline='') as f:
		writer = csv.writer(f)
		writer.writerow(['Workers', 'Execution Time 1', 'Execution Time 2', 'Execution Time 3'])
		writer.writerow(['2'] + res['2'])
		writer.writerow(['4'] + res['4'])
		writer.writerow(['6'] + res['6'])