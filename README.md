# Distributed Deduplication

## Goal
Use multiple kubernetes pods to deduplicate a dataset which is larger than the memory of any single pod.

## Result
Accomplish the goal, but not in a clean or easily reproducable manner.  Abandoned the project before wrapping it up neatly due to the complexity of testing.

## Design

![rudders drawio](https://user-images.githubusercontent.com/13628629/146095452-d9bb7e5c-773c-4134-b38e-f4f93248e21a.png)


## Motivation

I was frustrated with non-deterministic (dataset dependent) OOM errors in [Spark](https://spark.apache.org/docs/latest/) which were difficult to unit test.  This got me thinking about algorithims that adapt based on their memory usage (similar to a kubernetes horizontal pod autoscaler).

## Learnings

* A proxy is a much better design for this task
* Sending and receiving serialized data over the wire
  * started with TCP, but transitioned to gRPC
* Debugging and avoiding deadlocks
* Use of a kubernetes client libraries
* Building abstraction on top of a BTreeMap
* The challenges of making generic async traits
* Design a contract which is self refrencing and tollerates downtime
