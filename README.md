# KWQ - Kafka Worker Queue

##### HPC Compute Grid Worker Dispatcher/Scheduler
#

Many large firms rely upon large scale compute power to farm out millions of calculations across thousands of machines. The general term is a 'compute grid' and well established commercial products are 'Tibco DataSynapse GridServer' or 'IBM Platform Symphony. 

Banks use them for calculating 'risk', scientist use them for genomics and can be used for anything from surface modelling to chip manufacturinging. Anything requiring a large scale distributed calculation i.e. monte-carlo can be farmed out (at scale)

The goa of this project is to replace 2 parts of this infrastructure. 
1. The work-queue dispatcher - that actually allocates to work to remove workers
2. Task tracker - the provides a status as to the current workload and individual task status (i.e. how much stuff is running)

With any distributed system - the critical goals are related to reliability, scale and observability.



## Scheduler - KWQ

The worker queue is essentially a distributed priority queue that uses Kafka topics as individual prioritiy queues, and services those with the highest priority first. 
> See the SimpleKwq

## Task Tracker
This component is a Kafka topic/Stream that is written to when a Task is 'Allocated, Completed or Errored'

## Architecture
The general architecture is very simple

 

