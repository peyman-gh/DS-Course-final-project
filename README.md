
# DS-Course-final-project (revised)

  

## Steps to run the k8s (Minikube) cluster:

**Prerequisites**

Install Docker, Kubectl, Minikube, Helm.
then run these:
 
    minikube start
    minikube addons enable ingress
    


**Run kafka**

Navigate to project root directory and run:

		
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    helm install my-kafka bitnami/kafka -f k8s/kafka/values.yaml

**Build and run apps**

Build and tag the apps docker images:

    docker build -t data-generator:latest data-generator
    docker build -t data-generator-polygon:latest data-generator-polygon
    docker build -t ingestion:latest ingestion
    docker build -t analyze:latest analyze
    docker build -t notification:latest notification

Load images into minikube's docker env:

    minikube image load data-generator:latest
    minikube image load data-generator-polygon:latest
    minikube image load ingestion:latest
    minikube image load analyze:latest
    minikube image load notification:latest

Stop apps (if running)

    sh k8s-down.sh

Run apps:

    sh k8s-up.sh 



## Steps to run grafana:

    helm install my-grafana . -f helm-charts/grafana/values.yaml

    or 

    helm install my-grafana helm-charts/grafana -f helm-charts/grafana/values.yaml
    


You can connect to it by forwarding the port using the following command

    kubectl port-forward svc/my-grafana -n default 8080:80
    

## Steps to run postgresql:

    helm install my-pg helm-charts/postgresql -f helm-charts/postgresql/values.yaml

## Steps to run redis:

    helm install my-redis helm-charts/redis -f helm-charts/redis/values.yaml
