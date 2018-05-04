# KWQ - Kafka Worker Queue

##### HPC Compute Grid Worker Dispatcher/Scheduler
#

Many large firms rely upon large scale compute power to farm out millions of calculations across thousands of machines. The general term is a 'compute grid' and well established commercial products include 'Tibco DataSynapse GridServer' or 'IBM Platform Symphony. There are also several well known open-source implementations: BOINC, Condor etc...

Banks use them for calculating 'risk', scientist use them for genomics and can be used for anything from surface modelling to chip manufacturing. Anything requiring a large scale distributed calculation i.e. monte-carlo can be farmed out (at scale).

The goal of this project is to replace 2 parts of this infrastructure.
1. The work-queue dispatcher - that actually allocates to work to remove workers
2. Task tracker - that provides a status as to the current workload and individual task status (i.e. how much stuff is running)

With any distributed system - the critical goals are related to reliability, scale and observability.

## Motivation

So why bother with yet another implementation? 

In our experience, the HPC grid space is very old and as a result, most solutions try and be a 'kitchen sink' and solve all kinds of data, deployment and security related problems. They are also, not very cloud friendly. The reason for this project is to strip down the core functionality so that it can be used in conjunction with K8s, Containers, Cloud. It is platform agnostic and prevents lock-in by using the well established Apache Kafka for its ability to solve problems of throughput, horizontal scaling, partitioning, rebalancing and other good things etc.

Why not use Rabbit or some other message queueing tech?

Worker task dispatching to compute grids has a few nuances that aren't fully realised with the basic functionality of a message queue. The ability to retry tasks, use affinity to shape the flow of requests or queue prioritisation or the ability to scale out and run multiple instances. And then, you need a way of viewing progress and track task metrics by certain bespoke attributes.

## Features

- Prioritisation (i.e. fast lane different tasks)
- Tags to understand task payload and description - break down by Job or Tag (i.e. tradeid-10, coupon:10m,currency:USD,maturity:25y,risk:CR01,PV01)
- Timestamping of all stages of Task processing (Pending, Allocated,Running, Completed, Error)
>TODO
- Retry on error
- Retry count (i.e. < 3)
- Soft affinity (i.e. tasks can be sent to workers with matching 'soft' affinity keys when they are requesting work i.e. curve=USD. Allowing best effort reuse of state

## Scheduler - KWQ

The worker queue is essentially a distributed priority queue that uses Kafka topics as individual priority queues, and services those with the highest priority first. 
> See the SimpleKwq

## Task Tracker
This component is a Kafka topic/Stream that is written to when a Task is 'Allocated, Completed or Error'.

## Architecture
The general architecture is very simple.

 
 
 ## API

> Task

> Kwq


## Test driving the API via Swagger

> Accessible on http://localhost:9999/kwq

