#!/bin/bash
kubectl get pods | grep driver | awk '{ print "kubectl delete pod "$1 }' | bash