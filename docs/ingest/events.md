---
layout: default
parent: Data Ingest
nav_order: 4
---

# Event Messaging

Event messaging allows the system to respond and operate in near real-time while supporting scalability and extensibility for future use-cases. Designing around a messaging system provides a centralized mechanism for system components to communicate without coupling those components. That means new components to be added to the system ad-hoc and without redesigning core capabilities. Event-driven systems are able to scale by sending messages to many listeners at once. They also tend to distribute data through the system faster than batch systems because the event is raised immediately versus waiting a set increment of time.

**Key Points**
- An event is any change in data state, i.e. fundamentally new available data (new, updated, and deleted)
- Event systems make extending the system easier as requirements evolve
- Event-driven design results in data propagating through the system faster than traditional scheduled batch systems

## Methodology

The primary reason for architecting an event-driven system is the extensibility and scalability that it provides. Rather than having to track all possible uses of the data, each data consumer (with the goal of maintaining a data product) can subscribe to the event stream as its data source. Since many services can react to messages in their own way, using their own computing resources, this inherently supports system scalability. All of these services are then loosely-coupled and can operate and be maintained independently of one another.

The concept of using an event-driven data delivery system is not new. NOAA currently uses [Unidata's LDM service](https://www.unidata.ucar.edu/software/ldm/ldm-current/factsheet.html) to share data across the world using a stream of data events. The difference in this design, aside from the benefits of using a modern tech stack, is that the data itself doesn't need to be physically distributed because the data is centrally located on the cloud platform.

## Amazon Simple Notification Service (SNS)

We have prototyped messaging using SNS because we are able to receive messages directly from the NOAA data providers with minimal engineering effort. The messages are ephemeral so we use the Simple Queue Service (SQS) to capture those messages and then process through them in the queue. 

## RabbitMq

There are many modern messaging frameworks to choose from today. Every cloud platform provides their own brand of messaging (Amazon Simple Queue Service (SQS), Google Pub/Sub, and Azure Service Bus) and there are numerous open-source platforms as well. We initially prototyped RabbitMq as the messaging broker because it is relatively simple to configure, open source, and cloud platform independent. 

> RabbitMQ is the most widely deployed open source message broker. RabbitMQ is lightweight and easy to deploy on premises and in the cloud. It supports multiple messaging protocols. (https://www.rabbitmq.com/)

## Comparison

From a system architecture perspective, the main difference between SQS and RabbitMq is that SQS only works on AWS while RabbitMq will work on whatever platform it is installed on. However, SQS is already configured and "comes with" AWS natively. This is not really a limitation because if and when IOOS has the need to connect to other cloud providers (e.g. GCP, Azure), those new integrations can be developed without a major change to the underlying technical strategy. All of the major cloud providers support a messaging framework so there is no technical limitation, but each additional supported platform requires additional developer support and maintenance. On the other hand, using consistent tooling among cloud providers reduces the number of configurations and therefore platform-specific test-cases to be addressed. One could also argue that the maintenance and understanding of the RabbitMq system is another hidden developer cost that is not an issue in the managed services such as AWS SQS which is [maintained by Amazon 24/7](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/example-implementations-for-availability-goals.html) and just works.
