
# DS-Course-final-project

  

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
    docker build -t ingestion:latest ingestion
    docker build -t processing:latest processing

Load images into minikube's docker env:

    minikube image load data-generator:latest
    minikube image load ingestion:latest
    minikube image load processing:latest

Stop apps (if running)

    sh k8s-down.sh

Run apps:

    sh k8s-up.sh 

**See the latest json published to kafka topic**


get minikube ip:

    minikube ip

then visit http://minikube-ip:30585/temp

monitor pods and services:

    kubectl get pods
    kubectl get services


## Steps to run the Docker Compose version:

Start everything:

     docker compose up -d --build 

See the latest json published to kafka topic at http://localhost:30585/temp

Stop:

    docker compose stop

## Steps to run grafana:

    helm install my-grafana . -f helm-charts/grafana/values.yaml
    
You can connect to it by forwarding the port using the following command

    kubectl port-forward svc/my-grafana -n default 8080:80
    
