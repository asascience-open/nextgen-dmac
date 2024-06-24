---
layout: default
nav_order: 1
parent: Infrastructure
---

# Kubernetes

[Kubernetes](https://kubernetes.io/) (abbreviated as "K8s") is an infrastructure and container orchestration tool that enables organizations to effectively run and manage cloud-native services. It solves a number of challenges that organizations run into as they embark on their cloud-native journey. Like any technology solution, it must also be properly governed and managed to obtain the most benefit and avoid pitfalls. K8s is the best-in-class container management solution when an organization wants to maintain a flexible architecture that will run on any cloud platform without being tied directly to that cloud vendor. All of the major cloud vendors provide a managed Kubernetes service that greatly simplifies the deployment and configuration. For the purposes of this project, we deployed and managed a Kubernetes cluster using Amazon's Elastic Kubernetes Service (EKS).

## Key Considerations

We believe Kubernetes is a good technology for hosting next-generation infrastructure and services due to these features:

- Open source
- Heavily adopted across industries
- True cloud-agnostic platform: Kubernetes can be run on AWS, Azure, and GCP
  - Services running on K8s can be deployed to any cloud platform running K8s as-is
- Helps optimize cloud costs
- Centralized view of infrastructure

These are some of the technical problems that Kubernetes solves:

- Managing, provisioning, and scaling servers
- Managing and configuring the network infrastructure for those servers
- Managing many running containers
- Networking between interdependent containers
- Scaling services up and down based on workload
- Enables federated deployments on multiple cloud platforms

## Advantages and Drawbacks

Although Kubernetes is the best-in-class container management system, its powerful features also introduce complexity. The primary reason that we recommend Kubernetes for NOAA-IOOS is that it can significantly reduce architecture management challenges across the organization while maintaining a cloud-agnostic presence. It is much easier to switch cloud providers and avoid lock-in when deploying to Kubernetes as a platform because that platform can run on any cloud, including on-premise servers.

A trained Kubernetes administrator is needed to understand the proper methods for operating a Kubernetes cluster. Like in software design, a good K8s administrator will understand the proper architecture choices to solve various problems and avoid common pitfalls of designing an operational K8s system. Many vendors provide Kubernetes-as-a-service as well if outsourcing that infrastructure management is preferred.

Due to the scalable nature of K8s, one administrator can potentially manage hundreds of servers versus a traditional system administrator role which requires individual interactions with each server. Much of the burden of managing the operating system and configurations for each service now are managed by the developers using the power of containers. The containerized services can be deployed and managed on the cluster by developers or ideally a DevOps administrator without much burden on the K8s administrator. 

The number of K8s clusters run by an organization depends on the number of applications being deployed and how much isolation different processes need. There is no one way to deploy and manage K8s and the organization should evaluate their needs and operating policies to choose the correct architecture. [This article](https://learnk8s.io/how-many-clusters) describes many of the considerations for making those choices.

## Costs

The primary operating costs of a Kubernetes cluster are the price of the virtual machines needed to power it. Service providers also charge a small fee to use their managed K8s service.

These were the actual costs of running the prototype Kubernetes cluster:

| Service | Monthly Cost |
| ------- | ------------ |
| Elastic Container Service for K8s | $74.40 |
| Elastic Load Balancing | $16.74 |
| Other costs | $5.00 |
| 2x m5xlarge EC2 instances | $285.70 | 
| **Total** | **$381.84** |

The virtual machines cost about 75% of the total and would be needed regardless of using Kubernetes or not.

## Reliability and Uptime

While running our prototypes on the Kubernetes cluster we noted several advantages over a basic Docker platform:

- If a pod crashes, it gets reinitialized very quickly. While load testing, one pod kept crashing but it returned so quickly we didn't notice the delay at first.
- Patches and even changes to underlying hardware can occur with almost no downtime. K8s maintains the running state while spinning up new resources before transitioning them over to the new machines.
- The number of running pods was the biggest driver of resource comsumption. [This file](https://github.com/awslabs/amazon-eks-ami/blob/master/files/eni-max-pods.txt) describes how many pods each instance type can run
- The maximum number of pods is also limited by the number of available IP addresses
- If more pods are allocated on K8s than the hardware can support, it can crash the cluster due to too few available resources.