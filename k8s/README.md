# Kubernetes Configuration

-	`nlb.yaml` – defines the Network Load Balancer that receives external traffic and routes it to the Application Load Balancer
-	`ingress.yaml` – defines the Application Load Balancer
    -	Note that there is also an Application Load Balancer definition under /kerchunk/argo-events-ingress.yaml. These end up deploying as the same ALB resource, but under kerchunk it defines additional routes used by the Argo ingest process
    -	This implementation is using AWS Load Balancer Controller ingress.
-	`nginx-deployment.yaml` – defines the nginx proxy the performs the application routing to different services
- `install-load-balancer.sh` - script to show how to deploy the AWS Load Balancer Controller using Helm
    - Prereq: `aws-load-balancer-controller-service-account.yaml` - defines the Service Account used by the AWS Load Balancer Controller
- `xreds.yaml` - defines the xreds service
