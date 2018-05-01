# KWQ - Kafka Worker Queue

##### HPC Compute Grid Worker Dispatcher/Scheduler
#

Many large firms rely upon large scale compute power to farm out millions of calculations across thousands of machines. The general term is a 'compute grid' and well established commercial products include 'Tibco DataSynapse GridServer' or 'IBM Platform Symphony. There are also several well known open-source implementations: BOINC, Condor etc 

Banks use them for calculating 'risk', scientist use them for genomics and can be used for anything from surface modelling to chip manufacturing. Anything requiring a large scale distributed calculation i.e. monte-carlo can be farmed out (at scale)

The goa of this project is to replace 2 parts of this infrastructure. 
1. The work-queue dispatcher - that actually allocates to work to remove workers
2. Task tracker - the provides a status as to the current workload and individual task status (i.e. how much stuff is running)

With any distributed system - the critical goals are related to reliability, scale and observability.

## Motivation

So why bother with another implementation? 

In our experience, the HPC grid space is very old and as a result, most solutions try and be a 'kitchen sink' and solve all kinds of data, deployment and security related problems. They are also, not very cloud friendly. The reason for this project is to strip down the core functionality so that it can be used in conjunction with K8s, Containers, Cloud, it is platform agnostic and prevents lock-in by using the well established Apache Kafka for its ability to solve problems of throughput, horizontal scale etc.


## Scheduler - KWQ

The worker queue is essentially a distributed priority queue that uses Kafka topics as individual prioritiy queues, and services those with the highest priority first. 
> See the SimpleKwq

## Task Tracker
This component is a Kafka topic/Stream that is written to when a Task is 'Allocated, Completed or Errored'

## Architecture
The general architecture is very simple

 

