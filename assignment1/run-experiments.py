#!/home/cc-group2/miniconda3/bin/python3

import subprocess
from datetime import datetime
import os
import time
import csv
import sys

print("run experiments")

# create measurements folder
if os.path.isdir('./experiments/' + sys.argv[1] + '/measurements'):
    print('ERROR: measurements folder with same timestamp already exists')
    sys.exit(1)

os.mkdir('./experiments/' + sys.argv[1] + '/measurements')
values = list(map(lambda x: str(x/10), range(5,50,5)))
res = {file: {v: [] for v in values} for file in ['data-200MB']}
# startTimes = {file: {'2': [], '4': [], '6': [], '8': [], '10': []} for file in ['data-200MB']}

# carry out experiments
for i in range(0, 3):
	for file in ['data-200MB']:
		for testValue in values:
			# startTimes[file][executors].append(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
			out = subprocess.run(('/usr/bin/time --format %e ./spark-3.3.0-bin-hadoop3/bin/spark-submit \
				--master k8s://https://128.232.80.18:6443 \
				--deploy-mode cluster \
				--name group2-wordcount \
				--class com.cloudcomputing.group2.assignment1.App \
				--conf spark.dynamicAllocation.maxExecutors=10 \
				--conf spark.dynamicAllocation.executorAllocationRatio=' + testValue + '\
				--conf spark.kubernetes.namespace=cc-group2 \
				--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
				--conf spark.kubernetes.container.image=andylamp/spark:v3.3.0 \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
				--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
				--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
				local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/' + file + '.txt').split(),capture_output=True).stderr
			seconds = float(out.decode('utf-8').strip())
			print(file + " with " + testValue + " took " + str(seconds) + "s")
			res[file][testValue].append(seconds)
		# wait between files
		time.sleep(60)
	# delete pods
	os.system('kubectl get pods | grep driver | awk \'{ print "kubectl delete pod "$1 }\' | bash')

	if i==2:
		break
	# wait 15 mins before starting next iteration
	# time.sleep(14 * 60)
	
	# write to measurements
for file in ['data-200MB']:
	with open('./experiments/' + sys.argv[1] + '/measurements/' + file + '.csv', 'w', newline='') as f:
		writer = csv.writer(f)
		writer.writerow(['Workers', 'Execution Time 1', 'Execution Time 2', 'Execution Time 3'])
		for value in values:
			writer.writerow([value] + res[file][value])

# write to output2
# with open(f'output2-{sys.argv[1]}.csv', 'w', newline='') as f:
# 	writer = csv.writer(f)
# 	for file in ['data-200MB']:
# 		writer.writerow(['File/Workers', 'Execution Time 1', 'Execution Time 2', 'Execution Time 3'])
# 		writer.writerow([file+'/2'] + startTimes[file]['2'])
# 		writer.writerow([file+'/4'] + startTimes[file]['4'])
# 		writer.writerow([file+'/6'] + startTimes[file]['6'])
# 		writer.writerow([file+'/8'] + startTimes[file]['8'])
# 		writer.writerow([file+'/10'] + startTimes[file]['10'])
		
