
kubectl apply -f k8s/data-generator/deployment.yaml
kubectl apply -f k8s/ingestion/deployment.yaml
kubectl apply -f k8s/ingestion/service.yaml
kubectl apply -f k8s/processing/deployment.yaml
kubectl apply -f k8s/processing/service.yaml


#kubectl apply -f k8s/ingress/ingestion.yaml

# find k8s -name '*.yaml' -print0 | xargs -0 -I {} kubectl apply -f {}