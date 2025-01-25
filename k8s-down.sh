kubectl delete -f k8s/data-generator/deployment.yaml
kubectl delete -f k8s/data-generator-polygon/deployment.yaml
kubectl delete -f k8s/ingestion/deployment.yaml
kubectl delete -f k8s/ingestion/service.yaml
kubectl delete -f k8s/analyze/k8s.yaml
kubectl delete -f k8s/processing/service.yaml
kubectl delete -f k8s/notification/deployment.yaml
kubectl delete -f k8s/ingress/ingestion.yaml

# find k8s -name '*.yaml' -print0 | xargs -0 -I {} kubectl delete -f {}
