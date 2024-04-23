---
layout: default
title: Prototype Outcomes
nav_order: 3
---

# Prototype Outcomes

## Prototypes

| Component | Documentation | Code | Description |
| --------- | ---- | ------------  | ----------- |
| Kubernetes infrastructure | [Documentation](../architecture/kubernetes.md) | [Configuration](../../k8s/README.md) | Demonstrated and learned from running K8s in AWS as a potential computing platform
| RabbitMQ Messaging | [Documentation](./ingest/events.md#rabbitmq) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/messaging) | Demonstrates platform-independent messaging |
| JupyterHub w/ Dask on K8s | [Documentation](https://z2jh.jupyter.org/en/stable/) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/jupyterhub) | Initial experiments running Jupyterhub on K8s |
| RESTful Grids (2022 IOOS Code Sprint) | [Documentation](https://ioos.github.io/ioos-code-sprint/2022/topics/05-restful-services-for-gridded-data.html) | [Code](https://github.com/asascience-open/restful-grids) | Initial project investigating XPublish |
| Argo Workflows: Test QARTOD and IOOS Compliance Checker | [Documentation](https://github.com/asascience-open/nextgen-dmac/tree/main/qc_and_cchecker#readme) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/qc_and_cchecker) | Use of standard IOOS tools within a cloud workflow environment |
| Argo Workflows: Kerchunk Ingest for NOS model data | [Documentation](https://github.com/asascience-open/nextgen-dmac/tree/main/kerchunk#readme) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/kerchunk) | Executing kerchunk workflows within Argo Workflows platform |
| Lambda-based Kerchunk Ingest | [Documentation](./ingest/ingest-prototype.md) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/cloud_aggregator) | Executing kerchunk workflows using AWS Lambda and SNS/SQS |
| Pulumi Infrastructure-as-Code (infra for kerchunk jobs) | [Documentation](https://www.pulumi.com/docs/) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/cloud_aggregator#cloud-aggregator) | Using Pulumi to initialize cloud resources needed to run kerchunk ingest |
| Dask Gateway | [Documentation](https://gateway.dask.org/install-kube.html) | [Configuration](https://github.com/asascience-open/nextgen-dmac/tree/main/dask) | Running a Dask cluster on K8s |
| XPublish Web Mapping Service (WMS) | [Documentation](https://github.com/xpublish-community/xpublish-wms#readme) | [Code](https://github.com/xpublish-community/xpublish-wms) | Retrieve web map tiles directly from environmental data |
| HRRR Aggregator | [Documentation](https://github.com/asascience-open/nextgen-dmac/tree/main/hrrr_aggregator#readme) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/hrrr_aggregator) | Initial prototype to build large time-based aggregations by David Stuebe, Camus Energy |
| GRIB Index Aggregation | [Documentation](https://github.com/asascience-open/nextgen-dmac/tree/main/grib_index_aggregation#readme) | [Code](https://github.com/asascience-open/nextgen-dmac/tree/main/grib_index_aggregation) | Updated prototype to optimize large time-based aggregations for GRIB files by David Stuebe, Camus Energy |

## Demos and Notebooks

- [XREDS Data Viewer](https://nextgen-dev.ioos.us/xreds/)
- [XPublish Notebook Examples](https://github.com/asascience-open/nextgen-ioos-2023)

## Related Projects

- [NOAA Coastal Ocean Reanalysis (CORA)](../analysis/cora.md)
- [National Water Model](https://github.com/asascience-open/NWM)
- [HPC Cloud Subsetting](https://github.com/asascience-open/HPC-Cloud-Subsetting)
- [XPublish](https://xpublish.readthedocs.io/en/latest/)