mvn clean package
scp -i ~/.ssh/cloud ./target/assignment1-1.0-SNAPSHOT.jar cc-group2@caelum-109.cl.cam.ac.uk:~/yeet-data
ssh -i ~/.ssh/cloud cc-group2@caelum-109.cl.cam.ac.uk "kubectl cp ~/yeet-data/assignment1-1.0-SNAPSHOT.jar cc-group2/bobuntu:/volume-mount-yeet; ~/spark-3.3.0-bin-hadoop3/bin/spark-submit \
    --master k8s://https://128.232.80.18:6443 \
    --deploy-mode cluster \
    --name group2-wordcount \
    --class com.cloudcomputing.group2.assignment1.App \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.namespace=cc-group2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=andylamp/spark:v3.3.0 \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
    local:///test-data/assignment1-1.0-SNAPSHOT.jar /test-data/data-100MB.txt"