---
layout: default
nav_order: 1
parent: System Architecture
---

# Kubernetes

[Kubernetes](https://kubernetes.io/) (also abbreviated as "K8s") is an infrastructure and container orchestration tool that enables organizations to effectively run and manage cloud-native services. It solves a number of challenges that organizations run into as they embark on their cloud-native journey. Like any technology solution, it must also be properly governed and managed to obtain the most benefit and avoid pitfalls.

## Key Considerations

We believe Kubernetes is a good technology for hosting next-generation infrastructure and services due to these features:

- Open source
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