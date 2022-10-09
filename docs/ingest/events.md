---
layout: default
parent: Data Ingest
nav_order: 3
---

# Event Messaging

Event messaging allows the system to respond and operate in near real-time while supporting scalability and extensibility for future use-cases. Designing around a messaging system provides a centralized mechanism for system components to communicate without coupling those components. That means new components to be added to the system ad-hoc and without redesigning core capabilities. Event-driven systems are able to scale by sending messages to many listeners at once. They also tend to distribute data through the system faster than batch systems because the event is raised immediately versus waiting a set increment of time.

**Key Points**
- An event is any change in data state, fundamentally new available data
- Event systems make extending the system easier as requirements evolve
- Event-driven design results in data propagating through the system faster than traditional scheduled batch systems

## Methodology

The primary reason for architecting an event-driven system is the extensibility and scalability that it provides. Rather than having to track all possible uses of the data, each data consumer (with the goal of maintaining a data product) can subscribe to the event stream as its data source. Since many services can react to messages in their own way, using their own computing resources, this inherently supports system scalability. All of these services are then loosely-coupled and can operate and be maintained independently of one another.

The concept of using an event-driven data delivery system is not new. NOAA currently uses [Unidata's LDM service](https://www.unidata.ucar.edu/software/ldm/ldm-current/factsheet.html) to share data across the world using a stream of data events. The difference in this design, aside from the benefits of using a modern tech stack, is that the data itself doesn't need to be physically distributed because the data is centrally located on the cloud platform.

## RabbitMq

There are many modern messaging frameworks to choose from today. Every cloud platform provides their own brand of messaging (Amazon Simple Queue Service (SQS), Google Pub/Sub, and Azure Service Bus) and there are numerous open-source platforms as well. We are currently selecting RabbitMq as the messaging broker for prototyping because it is relatively simple to configure, open source, and cloud platform independent.

> RabbitMQ is the most widely deployed open source message broker. RabbitMQ is lightweight and easy to deploy on premises and in the cloud. It supports multiple messaging protocols. (https://www.rabbitmq.com/)