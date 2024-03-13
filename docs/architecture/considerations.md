---
layout: default
nav_order: 3
parent: System Architecture
---

# Considerations

The primary consideration when choosing a cloud infrastructure platform mainly is how much control one wants over fine details of the system, and therefore how much responsibility the organization is willing to own in maintaining that infrastructure. The spectrum of choices is highlighted in this diagram:

![Cloud services](../assets/cloud-services.png)

Generally, Traditional On-Premises will cost the most since the organization will need to fund the entire stack, whereas cloud providers are able to amortize that investment across many customers and charge fees for resource usage. This isn't necessarily a linear comparison and depends highly on the use-case as to which architecture is right for the application. For example, a very heavily used Function-as-a-Service may incur significant costs, and in that case might be better running on a dedicated machine using Containers-as-a-Service. There are also limitations using Functions or Platforms such as maximum memory or network bandwidth, so some solutions necessitate more management.


## Recommendations

Each of the 11 Regional Associations (RAs), as part of NOAA-IOOS, operate independent infrastructure to process, store, and serve their data. The RAs mainly operate Traditional On-Premises and Infrastructure-as-a-Service (such as VMWare) configurations.

From a cost-savings perspective...

![System Architecture recommendation](../assets/overall-architecture-rec.png)