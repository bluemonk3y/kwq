# KWQ - Kafka Worker Queue

##### HPC Compute Grid Worker Dispatcher/Scheduler

##### Note: this project is functional but not complete, for example - pause is not implemented - feel free to contribute

Many large firms rely upon large scale compute power to farm out millions of calculations across thousands of machines. The general term is a 'compute grid' and well established commercial products include 'Tibco DataSynapse GridServer' or 'IBM Platform Symphony. There are also several well known open-source implementations: BOINC, Condor etc 

Banks use them for calculating 'risk', scientist use them for genomics and can be used for anything from aerodynamic surface modelling to chip manufacturing. Anything requiring a large scale distributed, analytic calculations i.e. monte-carlo can be farmed out (at scale)

The goal of this project is to replace the grid scheduler with 2 components 
1. The work-queue dispatcher - it dispatches tasks from a priority queue to workers. (a single job queue can have up to 50 millions tasks)
2. Task tracker - the provides a status as to the current workload and individual task status (i.e. how much stuff is running)

With any distributed system - the critical goals are related to reliability, scale and observability.

## Motivation

So why bother with yet another implementation, especially when there are so many available? 

In our experience, the HPC grid space is very old and as a result, most solutions try and be a 
'kitchen sink' and solve all kinds of data distribution, library deployment scaling and security related problems. 
They are also not very cloud friendly. Why do we still need all of that junk the new generation of tooling handle it much better? 

The reason for this project is to strip down the core functionality so that it can be used in conjunction with K8s, Containers, Cloud. It is platform agnostic and prevents lock-in by using the well established Apache Kafka for its ability to solve problems of throughput, horizontal scaling, partitioning, rebalancing; all the good things etc.

Why not use Rabbit or some other message queueing tech?

Worker task dispatching to compute grids has a few nuances that aren't fully realised with the 
basic functionality of a message queue. The ability to retry tasks, use affinity to shape the flow 
of requests or queue prioritisation or the ability to scale out and run multiple instances. 
And then, you need a way of viewing progress and track task metrics by certain bespoke attributes.

What is probably the most interesting aspect, is that when you break it down to this level of 
granularity - a worker queue scheduler is a akin to those used to power FaaS like scheduling... interesting. The Task.payload could be used to run docker images

## Features

- Prioritisation (i.e. fast lane different tasks)
- Tags to understand task payload and description - break down by Job or Tag (i.e. tradeid-10, coupon:10m,currency:USD,maturity:25y,risk:CR01,PV01)
- Timestamping of all stages of Task processing (Pending, Allocated,Running, Completed, Error)
>TODO
- Retry on error (i.e. > 3)
- Retry count tracking
- Soft affinity (i.e. tasks can be sent to workers with matching 'soft' affinity keys when work is requested i.e. curve=USD. Supporting best effort reuse of worker-state

### Features we get for free (from Kafka)

- The ability to run multiple scheduler instances and scale horizontally
- Persistance and replayability of events
- Super fast throughput


## Architecture
The general architecture contains a REST Endpoint for submitting or consuming tasks. Tasks can alternatively be submitted directly onto a KWQ-priority-X topic.
Workers request tasks from the same REST endpoint and the scheduler (KWQ) updates the status in the Task Tracker. When a task is 'complete' or 'error' then the worker can notify the Task Tracker via the REST endpoint.

#### Scheduler - KWQ

The worker queue is essentially a distributed priority queue that uses Kafka topics as individual priority queues, and services those with the highest priority first. 
> See io.confluent.kwq.SimpleKwq.java

#### Task Tracker
This component is a Kafka Stream processor that tracks when a where Tasks are 'Allocated, Completed or Error'
> See io.confluent.kwq.TaskStatusImpl.java

### Worker Simulator
This component drives the KWQ & TaskStatus interfaces - consuming tasks and updating task status
> See io.confluent.kwq.Simulator.java

### API

View the API by navigating to the Swagger endpoint: http://localhost:8080/swagger/index.html
From there you can understand how to submit() tasks consume() tasks and update task status

## Get Running!

Build the jar (and run tests)
> mvn package

OR
> mvn clean package -DskipTests

Expand **target/kwq-0.1-SNAPSHOT-dist.zip** to a destination folder and run the KwqRestServer against your brokers (defaults to localhost:9092)
>java -cp "kwq-0.1-SNAPSHOT.jar:lib/*" -Dbootstrap.servers=localhost:9092 io.confluent.kwq.KwqRestServerMain

Alternatively, modify the kwq.sh file (as above) apply execute permission and then execute it. 

>chmod +x kwq.sh

>./kwq.sh &

To view the logging configuration file being used when starting KWQ:
> -Dlog4j.debug=true

*Log files will appear in the ./log/ directory - see log4j.properties for configuration*

## Within your IDE

> Run java class  io.confluent.kwq.KwqRestServerMain with System property: -Dkwq.resources.folder=./src/main/resources 

 ## User interface and Endpoints
 - ADMIN UI: http://localhost:8080/ui/index.html
 - SWAGGER: http://localhost:8080/swagger/index.html Run the task simulator and interact with the REST API 
 - REST: http://localhost:8080/kwq 
 - OPEN-API-SPEC: http://localhost:8080/openapi.json
 

## Running a simulation

1. > Navigate to the Swagger interface
**http://localhost:8080/swagger/index.html**

2. > Execute **kwq/simulate/{numberOfTasks}/{durationSeconds}/{numberOfWorkers}**

3. > Navigate to the UI to view throughput and status: http://localhost:8080/ui/index.html

4. > Check the kwq.log file for output from the simulator

