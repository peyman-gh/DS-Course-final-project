#!/bin/sh

kubectl delete -f ../k8s/analyze/k8s.yaml
sleep 20
minikube image rm analyze:latest
docker image rm analyze:latest
docker build -t analyze:latest .
minikube image load analyze:latest
kubectl apply -f ../k8s/analyze/k8s.yaml

pod_name=$(kubectl get pods | grep -v NAME | grep analyze | awk '{print $1;}')
echo ${pod_name}
kubectl logs ${pod_name}
